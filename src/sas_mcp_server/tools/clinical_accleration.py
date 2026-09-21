# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tier 10 — Clinical Acceleration tools (SAS Clinical Acceleration Repository)."""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import httpx
from fastmcp import Context, FastMCP
from pydantic import BeforeValidator

from ..config import MAX_EXPORT_INLINE_BYTES, VIYA_ENDPOINT
from ..helpers import clinical_access_helpers as access
from ..viya_client import (
    filter_literal,
    get_json,
    get_paged_items,
    post_json,
    raise_for_viya_status,
    return_items,
)
from ._common import coerce_json_dict, coerce_json_list, make_session_helpers

PrincipalList = Annotated[list[dict[str, Any]], BeforeValidator(coerce_json_list)]
PermissionEntryList = Annotated[list[dict[str, Any]], BeforeValidator(coerce_json_list)]
StringList = Annotated[list[str], BeforeValidator(coerce_json_list)]
AccessModelParam = Annotated[dict[str, Any], BeforeValidator(coerce_json_dict)]

_REPO = "/clinicalRepository"
_REPO_ITEMS = f"{_REPO}/repository/items"
_REPO_PATHS = f"{_REPO}/repository/paths"
_WORKSPACE_FILES = f"{_REPO}/workspaces/@currentUser/files"
_WORKSPACE_ITEMS = f"{_REPO}/workspaces/@currentUser/items"
_WORKSPACE_CONTENT = f"{_REPO}/workspaces/@currentUser/items/content"
_WORKFLOW_TASKS = f"{_REPO}/workflowTasks"
_AUDIT_ENTRIES = f"{_REPO}/audit/entries"
_MEMBERSHIPS = f"{_REPO}/memberships"
_GROUPS = f"{_REPO}/groups"
_ROLES = f"{_REPO}/roles"

_PRINCIPAL_FIELDS = ["id", "typeId", "name", "displayName"]
_GROUP_FIELDS = ["id", "name", "description", "displayName"]
_ROLE_FIELDS = [
    "id",
    "name",
    "displayName",
    "description",
    "inherited",
    "definedContextId",
    "assignedContextId",
]
_PRIVILEGE_FIELDS = ["id", "name", "description", "typeId"]

_PRIMARY_TYPES = frozenset({"FILE", "FOLDER", "CONTEXT"})
_ITEM_FIELDS = [
    "id",
    "name",
    "primaryType",
    "typeId",
    "path",
    "location",
    "size",
    "state",
    "description",
    "locked",
    "versioned",
    "fileVersion",
]


def _and_filter(clauses: list[str]) -> str | None:
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return f"and({','.join(clauses)})"


def _normalize_path(path: str) -> str:
    """Normalize a repository or workspace path (leading slash, no trailing slash)."""
    cleaned = path.strip()
    if not cleaned:
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    if cleaned != "/" and cleaned.endswith("/"):
        cleaned = cleaned.rstrip("/")
    return cleaned


def _item_type_clause(item_type: str) -> str:
    raw = item_type.strip()
    upper = raw.upper()
    if upper in _PRIMARY_TYPES:
        return f"eq(primaryType,'{upper}')"
    return f"eq(typeId,'{filter_literal(raw)}')"


def _query_clause(query: str, *, search_content: bool) -> str | None:
    text = query.strip()
    if not text:
        return None
    name = f"contains(name,'{filter_literal(text)}')"
    if not search_content:
        return name
    description = f"contains(description,'{filter_literal(text)}')"
    return f"or({name},{description})"


def _summarize_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        return []
    return return_items(items, _ITEM_FIELDS)


def _decode_download(resp: httpx.Response) -> dict[str, Any]:
    """Shape a content download for MCP: text when possible, else base64."""
    raw = resp.content
    size = len(raw)
    content_type = resp.headers.get("content-type", "application/octet-stream")
    if size > MAX_EXPORT_INLINE_BYTES:
        return {
            "status": "too_large",
            "size_bytes": size,
            "limit_bytes": MAX_EXPORT_INLINE_BYTES,
            "content_type": content_type,
            "message": (
                f"Download is {size} bytes, above the inline limit of "
                f"{MAX_EXPORT_INLINE_BYTES}. Narrow the request or raise "
                "MAX_EXPORT_INLINE_BYTES."
            ),
        }
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "status": "ok",
            "encoding": "base64",
            "content_type": content_type,
            "size_bytes": size,
            "content": base64.b64encode(raw).decode("ascii"),
        }
    return {
        "status": "ok",
        "encoding": "text",
        "content_type": content_type,
        "size_bytes": size,
        "content": text,
    }


def _resolve_upload_bytes(
    content: str | None,
    content_base64: str | None,
) -> bytes:
    if (content is None) == (content_base64 is None):
        raise ValueError("provide exactly one of content or content_base64")
    if content is not None:
        return content.encode("utf-8")
    assert content_base64 is not None
    try:
        return base64.b64decode(content_base64, validate=True)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("content_base64 is not valid base64") from exc


async def _workspace_file_action(
    client: httpx.AsyncClient,
    *,
    path: str,
    action: str,
    file_version: str | None = None,
    comment: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"path": _normalize_path(path), "action": action}
    if file_version:
        params["fileVersion"] = file_version
    if comment:
        params["comment"] = comment
    resp = await client.post(
        f"{VIYA_ENDPOINT}{_WORKSPACE_FILES}",
        params=params,
        headers={"Accept": "application/json"},
    )
    raise_for_viya_status(resp)
    if not resp.content:
        return {"status": "ok", "action": action, "path": params["path"]}
    data = resp.json()
    if isinstance(data, dict):
        data.setdefault("action", action)
        return data
    return {"status": "ok", "action": action, "path": params["path"], "result": data}


def register(mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]) -> None:
    """Register Tier 10 (Clinical Acceleration) tools on *mcp*."""

    viya_session, _ = make_session_helpers(get_token)

    # --- discover / navigate -------------------------------------------------

    @mcp.tool()
    async def search_clinical_repository(
        query: str,
        ctx: Context,
        context_path: str | None = None,
        item_type: str | None = None,
        search_content: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Search the SAS Clinical Acceleration Repository for contexts, folders, and files.

        Calls ``GET /clinicalRepository/repository/items`` with a Viya filter.

        Args:
            query: Name substring (and description when ``search_content`` is true).
                Empty string lists under ``context_path`` / ``item_type`` only.
            context_path: Optional path prefix (``startsWith(path, ...)``).
            item_type: ``FILE`` / ``FOLDER`` / ``CONTEXT``, or a type id.
            search_content: Also match description metadata (not file-body text).
            limit: Maximum items to return (default 50).
        """
        clauses: list[str] = []
        query_part = _query_clause(query, search_content=search_content)
        if query_part:
            clauses.append(query_part)
        if context_path and context_path.strip():
            clauses.append(
                f"startsWith(path,'{filter_literal(_normalize_path(context_path))}')"
            )
        if item_type and item_type.strip():
            clauses.append(_item_type_clause(item_type))

        async with viya_session("search_clinical_repository", ctx) as client:
            items, _ = await get_paged_items(
                _REPO_ITEMS,
                client,
                limit=limit,
                filters=_and_filter(clauses),
                extra_params={"sortBy": "name:ascending:primary"},
            )
            return _summarize_items(items)

    @mcp.tool()
    async def get_clinical_item(item_id: str, ctx: Context) -> dict[str, Any]:
        """Get one Clinical Repository item by id.

        Args:
            item_id: Repository item UUID.
        """
        async with viya_session("get_clinical_item", ctx) as client:
            return await get_json(f"{_REPO_ITEMS}/{item_id}", client)

    @mcp.tool()
    async def get_clinical_item_by_path(
        path: str,
        ctx: Context,
        file_version: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a Clinical Repository item by full path.

        Args:
            path: Full repository path (e.g. ``/StudyA/programs/adsl.sas``).
            file_version: Optional version when the path is a versioned file.
        """
        params: dict[str, Any] = {"path": _normalize_path(path)}
        if file_version:
            params["fileVersion"] = file_version
        async with viya_session("get_clinical_item_by_path", ctx) as client:
            return await get_json(_REPO_PATHS, client, params=params)

    @mcp.tool()
    async def list_clinical_children(
        item_id: str,
        ctx: Context,
        recurse: bool = False,
        limit: int = 100,
        filter_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List children of a Clinical Repository context or folder.

        Args:
            item_id: Parent item UUID.
            recurse: When true, include all descendants (default false).
            limit: Maximum items to return (default 100).
            filter_name: Optional name substring filter.
        """
        filters = (
            f"contains(name,'{filter_literal(filter_name)}')" if filter_name else None
        )
        async with viya_session("list_clinical_children", ctx) as client:
            items, _ = await get_paged_items(
                f"{_REPO_ITEMS}/{item_id}/children",
                client,
                limit=limit,
                filters=filters,
                extra_params={
                    "recurse": str(recurse).lower(),
                    "sortBy": "name:ascending:primary",
                },
            )
            return _summarize_items(items)

    # --- structure / content -------------------------------------------------

    @mcp.tool()
    async def create_clinical_folder(
        parent_item_id: str,
        name: str,
        ctx: Context,
        item_type: str = "FOLDER",
        type_id: str | None = None,
        description: str | None = None,
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Create a folder or context under a Clinical Repository parent.

        Args:
            parent_item_id: Parent container UUID.
            name: Name of the new item.
            item_type: ``FOLDER`` (default) or ``CONTEXT``.
            type_id: Required when creating a ``CONTEXT`` (e.g. ``project``).
            description: Optional description.
            owner: Optional owner user id (contexts only).
        """
        primary = item_type.strip().upper()
        if primary not in {"FOLDER", "CONTEXT"}:
            raise ValueError("item_type must be FOLDER or CONTEXT")
        if primary == "CONTEXT" and not (type_id and type_id.strip()):
            raise ValueError("type_id is required when creating a CONTEXT")
        params: dict[str, Any] = {"name": name, "type": primary}
        if type_id:
            params["typeId"] = type_id
        if description:
            params["description"] = description
        if owner:
            params["owner"] = owner
        async with viya_session("create_clinical_folder", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{parent_item_id}/children",
                params=params,
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "created", "name": name}

    @mcp.tool()
    async def upload_clinical_file(
        item_id: str,
        ctx: Context,
        file_name: str | None = None,
        content: str | None = None,
        content_base64: str | None = None,
        file_version: str | None = None,
        comment: str | None = None,
        expand: bool = False,
    ) -> dict[str, Any]:
        """Upload or update a file in the Clinical Repository.

        ``item_id`` is either an existing file (content replaced / new version) or
        a container (file created under it). Provide exactly one of ``content``
        (UTF-8 text) or ``content_base64`` (binary).

        Args:
            item_id: Target file or parent container UUID.
            file_name: File name when uploading into a container.
            content: Inline text content.
            content_base64: Inline binary content, base64-encoded.
            file_version: ``MAJOR``, ``MINOR``, or a version like ``1.0``.
            comment: Optional version comment.
            expand: When true and uploading a ZIP to a container, expand it.
        """
        file_bytes = _resolve_upload_bytes(content, content_base64)
        name = file_name or "upload.bin"
        params: dict[str, Any] = {}
        if file_name:
            params["name"] = file_name
        if file_version:
            params["fileVersion"] = file_version
        if comment:
            params["comment"] = comment
        if expand:
            params["expand"] = "true"
        async with viya_session("upload_clinical_file", ctx) as client:
            resp = await client.put(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{item_id}/content",
                params=params,
                files={"file": (name, file_bytes, "application/octet-stream")},
                data={"filename": name},
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            if not resp.content:
                return {"status": "ok", "item_id": item_id, "file_name": name}
            return resp.json()

    @mcp.tool()
    async def download_clinical_file(
        item_id: str,
        ctx: Context,
        child_names: list[str] | None = None,
    ) -> dict[str, Any]:
        """Download Clinical Repository file or folder content.

        For a file, returns the latest content. For a container, returns a ZIP
        (optionally limited to ``child_names``).

        Args:
            item_id: File or container UUID.
            child_names: Optional names inside a container to include in the ZIP.
        """
        async with viya_session("download_clinical_file", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{item_id}/content",
                json=child_names or None,
                headers={"Accept": "*/*", "Content-Type": "application/json"},
            )
            raise_for_viya_status(resp)
            result = _decode_download(resp)
            result["item_id"] = item_id
            return result

    @mcp.tool()
    async def list_clinical_file_versions(
        file_id: str,
        ctx: Context,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List version history for a Clinical Repository file.

        Args:
            file_id: File item UUID.
            limit: Maximum versions to return (default 50).
        """
        async with viya_session("list_clinical_file_versions", ctx) as client:
            items, _ = await get_paged_items(
                f"{_REPO_ITEMS}/{file_id}/versions",
                client,
                limit=limit,
            )
            if not items:
                return []
            return return_items(
                items,
                [
                    "fileVersion",
                    "creationTimeStamp",
                    "createdBy",
                    "comment",
                    "size",
                    "latest",
                    "path",
                ],
            )

    @mcp.tool()
    async def download_clinical_file_version(
        file_id: str,
        file_version: str,
        ctx: Context,
    ) -> dict[str, Any]:
        """Download a specific version of a Clinical Repository file.

        Args:
            file_id: File item UUID.
            file_version: Version id (e.g. ``1.0``, ``2.1``).
        """
        async with viya_session("download_clinical_file_version", ctx) as client:
            resp = await client.get(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{file_id}/versions/{file_version}/content",
                headers={"Accept": "*/*"},
            )
            raise_for_viya_status(resp)
            result = _decode_download(resp)
            result["file_id"] = file_id
            result["file_version"] = file_version
            return result

    # --- workspace + check-in / check-out ------------------------------------

    @mcp.tool()
    async def get_clinical_workspace_item(path: str, ctx: Context) -> dict[str, Any]:
        """Get an item from the current user's Clinical workspace by path.

        Args:
            path: Full workspace path.
        """
        async with viya_session("get_clinical_workspace_item", ctx) as client:
            return await get_json(
                _WORKSPACE_ITEMS,
                client,
                params={"path": _normalize_path(path)},
            )

    @mcp.tool()
    async def upload_clinical_workspace_file(
        path: str,
        ctx: Context,
        content: str | None = None,
        content_base64: str | None = None,
        file_name: str | None = None,
        expand: bool = False,
    ) -> dict[str, Any]:
        """Upload or update a file in the current user's Clinical workspace.

        Args:
            path: Workspace file path, or parent folder path when creating.
            content: Inline text content.
            content_base64: Inline binary content, base64-encoded.
            file_name: Multipart file name (defaults to the path basename).
            expand: Expand a ZIP when ``path`` is a folder.
        """
        file_bytes = _resolve_upload_bytes(content, content_base64)
        name = file_name or _normalize_path(path).rsplit("/", 1)[-1] or "upload.bin"
        params: dict[str, Any] = {"path": _normalize_path(path)}
        if expand:
            params["expand"] = "true"
        async with viya_session("upload_clinical_workspace_file", ctx) as client:
            resp = await client.put(
                f"{VIYA_ENDPOINT}{_WORKSPACE_ITEMS}",
                params=params,
                files={"file": (name, file_bytes, "application/octet-stream")},
                data={"filename": name},
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            if not resp.content:
                return {"status": "ok", "path": params["path"], "file_name": name}
            return resp.json()

    @mcp.tool()
    async def download_clinical_workspace(
        paths: list[str],
        ctx: Context,
    ) -> dict[str, Any]:
        """Download one or more paths from the current user's Clinical workspace.

        A single file path returns that file; multiple paths or a folder return a ZIP.

        Args:
            paths: Workspace paths to download.
        """
        if not paths:
            raise ValueError("paths must contain at least one workspace path")
        body = {
            "version": 1,
            "paths": [_normalize_path(p) for p in paths if p and p.strip()],
        }
        if not body["paths"]:
            raise ValueError("paths must contain at least one workspace path")
        async with viya_session("download_clinical_workspace", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_WORKSPACE_CONTENT}",
                json=body,
                headers={
                    "Accept": "*/*",
                    "Content-Type": "application/json",
                },
            )
            raise_for_viya_status(resp)
            result = _decode_download(resp)
            result["paths"] = body["paths"]
            return result

    @mcp.tool()
    async def checkout_clinical_file(path: str, ctx: Context) -> dict[str, Any]:
        """Check out a repository file and copy it into the user's workspace.

        Creates missing parent folders in the workspace. Overwrites an existing
        workspace copy of the same path.

        Args:
            path: Full repository/workspace-relative path of the file.
        """
        async with viya_session("checkout_clinical_file", ctx) as client:
            return await _workspace_file_action(
                client, path=path, action="CHECK_OUT"
            )

    @mcp.tool()
    async def checkout_clinical_file_metadata_only(
        path: str,
        ctx: Context,
    ) -> dict[str, Any]:
        """Check out a repository file without copying content into the workspace.

        Args:
            path: Full path of the file to check out.
        """
        async with viya_session("checkout_clinical_file_metadata_only", ctx) as client:
            return await _workspace_file_action(
                client, path=path, action="CHECK_OUT_WITHOUT_COPY"
            )

    @mcp.tool()
    async def undo_clinical_checkout(path: str, ctx: Context) -> dict[str, Any]:
        """Undo a checkout you own. Workspace content is left unchanged.

        Args:
            path: Full path of the checked-out file.
        """
        async with viya_session("undo_clinical_checkout", ctx) as client:
            return await _workspace_file_action(
                client, path=path, action="UNDO_CHECKOUT"
            )

    @mcp.tool()
    async def checkin_clinical_file(
        path: str,
        ctx: Context,
        file_version: str | None = None,
        comment: str | None = None,
    ) -> dict[str, Any]:
        """Check in a workspace file to the Clinical Repository.

        Creates or updates the repository file and any missing parent folders.
        Works for an already-checked-out file or a new workspace-only file.

        Args:
            path: Full workspace path of the file.
            file_version: ``MAJOR``, ``MINOR``, or a specific version (e.g. ``1.0``).
            comment: Optional version comment.
        """
        async with viya_session("checkin_clinical_file", ctx) as client:
            return await _workspace_file_action(
                client,
                path=path,
                action="CHECK_IN",
                file_version=file_version,
                comment=comment,
            )

    @mcp.tool()
    async def copy_clinical_file_to_workspace(
        path: str,
        ctx: Context,
        file_version: str | None = None,
    ) -> dict[str, Any]:
        """Copy a repository file into the workspace without checking it out.

        Args:
            path: Full path of the repository file.
            file_version: Optional specific version; omit for the latest.
        """
        action = "COPY_VERSION" if file_version else "COPY_LATEST_VERSION"
        async with viya_session("copy_clinical_file_to_workspace", ctx) as client:
            return await _workspace_file_action(
                client,
                path=path,
                action=action,
                file_version=file_version,
            )

    # --- workflow tasks + audit ----------------------------------------------

    @mcp.tool()
    async def list_clinical_tasks(
        ctx: Context,
        filter_expr: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List Clinical Acceleration workflow tasks.

        Args:
            filter_expr: Optional Viya filter expression.
            limit: Maximum tasks to return after fetch (default 50).
        """
        params: dict[str, Any] = {}
        if filter_expr:
            params["filter"] = filter_expr
        async with viya_session("list_clinical_tasks", ctx) as client:
            data = await get_json(_WORKFLOW_TASKS, client, params=params or None)
            items = data.get("items", []) or []
            trimmed = items[: max(0, limit)]
            if not trimmed:
                return []
            return return_items(
                trimmed,
                ["id", "name", "state", "assigneeId", "workflowId", "creationTimeStamp"],
            )

    @mcp.tool()
    async def start_clinical_task(task_id: str, ctx: Context) -> dict[str, Any]:
        """Start a Clinical Acceleration workflow task.

        Args:
            task_id: Task UUID.
        """
        async with viya_session("start_clinical_task", ctx) as client:
            resp = await client.put(
                f"{VIYA_ENDPOINT}{_WORKFLOW_TASKS}/{task_id}/startedTasks",
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "started", "task_id": task_id}

    @mcp.tool()
    async def complete_clinical_task(
        task_id: str,
        ctx: Context,
        comment: str | None = None,
        hours_worked: float | None = None,
    ) -> dict[str, Any]:
        """Complete a Clinical Acceleration workflow task.

        Args:
            task_id: Task UUID.
            comment: Optional completion comment.
            hours_worked: Optional hours worked.
        """
        body: dict[str, Any] = {}
        if comment is not None:
            body["comment"] = comment
        if hours_worked is not None:
            body["hoursWorked"] = hours_worked
        async with viya_session("complete_clinical_task", ctx) as client:
            return await post_json(
                f"{_WORKFLOW_TASKS}/{task_id}/completedTask",
                client,
                body=body,
            )

    @mcp.tool()
    async def list_clinical_audit_entries(
        ctx: Context,
        filter_expr: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List Clinical Repository audit entries (typically requires admin rights).

        Args:
            filter_expr: Optional Viya filter expression.
            limit: Maximum entries to return (default 50).
        """
        async with viya_session("list_clinical_audit_entries", ctx) as client:
            items, _ = await get_paged_items(
                _AUDIT_ENTRIES,
                client,
                limit=limit,
                filters=filter_expr,
                extra_params={"sortBy": "timestamp:descending:primary"},
            )
            if not items:
                return []
            return return_items(
                items,
                [
                    "id",
                    "action",
                    "timestamp",
                    "userId",
                    "sourceName",
                    "location",
                    "description",
                ],
            )

    # --- membership ----------------------------------------------------------

    @mcp.tool()
    async def get_clinical_membership(context_id: str, ctx: Context) -> dict[str, Any]:
        """Get membership metadata for a Clinical context (or nearest membership context).

        Args:
            context_id: Context (or item) UUID used as the membership id.
        """
        async with viya_session("get_clinical_membership", ctx) as client:
            return await get_json(f"{_MEMBERSHIPS}/{context_id}", client)

    @mcp.tool()
    async def list_clinical_context_members(
        context_id: str,
        ctx: Context,
        assigned_only: bool | None = None,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List members of a Clinical context.

        Args:
            context_id: Context UUID.
            assigned_only: If true, only assigned users/groups; if false, only
                groups created at the context; if omitted, all members.
            limit: Maximum members to return (default 100).
            filter_expr: Optional Viya filter expression.
        """
        extra: dict[str, Any] = {}
        if assigned_only is not None:
            extra["assignedOnly"] = str(assigned_only).lower()
        async with viya_session("list_clinical_context_members", ctx) as client:
            items, _ = await get_paged_items(
                f"{_MEMBERSHIPS}/{context_id}/members",
                client,
                limit=limit,
                filters=filter_expr,
                extra_params=extra or None,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_member_candidates(
        context_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List users/groups that can be added to a context membership.

        Args:
            context_id: Context UUID.
            limit: Maximum candidates (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_member_candidates", ctx) as client:
            items, _ = await get_paged_items(
                f"{_MEMBERSHIPS}/{context_id}/members/candidates",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def update_clinical_context_members(
        context_id: str,
        ctx: Context,
        add_members: PrincipalList | None = None,
        remove_members: PrincipalList | None = None,
    ) -> dict[str, Any]:
        """Add or remove members on a Clinical context.

        Args:
            context_id: Context UUID.
            add_members: Principals to add, each ``{id, typeId}`` (``user``|``group``).
            remove_members: Principals to remove, same shape.
        """
        body = access.members_update_body(add_members, remove_members)
        async with viya_session("update_clinical_context_members", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_MEMBERSHIPS}/{context_id}",
                patch_url=f"{_MEMBERSHIPS}/{context_id}/members",
                params={"action": "UPDATE_MEMBERS"},
                body=body,
                content_type="application/vnd.sas.clinical.members.update+json",
            )

    # --- groups --------------------------------------------------------------

    @mcp.tool()
    async def list_clinical_groups(
        context_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List groups for a Clinical context.

        Args:
            context_id: Context UUID.
            limit: Maximum groups (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_groups", ctx) as client:
            items, _ = await get_paged_items(
                _GROUPS,
                client,
                limit=limit,
                filters=filter_expr,
                extra_params={"contextId": context_id},
            )
            return return_items(items, _GROUP_FIELDS) if items else []

    @mcp.tool()
    async def get_clinical_group(group_id: str, ctx: Context) -> dict[str, Any]:
        """Get one Clinical group by id.

        Args:
            group_id: Group UUID.
        """
        async with viya_session("get_clinical_group", ctx) as client:
            return await get_json(f"{_GROUPS}/{group_id}", client)

    @mcp.tool()
    async def list_clinical_group_members(
        group_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List members of a Clinical group.

        Args:
            group_id: Group UUID.
            limit: Maximum members (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_group_members", ctx) as client:
            items, _ = await get_paged_items(
                f"{_GROUPS}/{group_id}/members",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_group_member_candidates(
        group_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List candidates that can be added to a Clinical group.

        Args:
            group_id: Group UUID.
            limit: Maximum candidates (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_group_member_candidates", ctx) as client:
            items, _ = await get_paged_items(
                f"{_GROUPS}/{group_id}/members/candidates",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def create_clinical_group(
        context_id: str,
        name: str,
        ctx: Context,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a group on a Clinical context.

        Args:
            context_id: Context UUID.
            name: Group name.
            description: Optional description.
        """
        params: dict[str, Any] = {"contextId": context_id, "name": name}
        if description:
            params["description"] = description
        async with viya_session("create_clinical_group", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_GROUPS}",
                params=params,
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "created", "name": name}

    @mcp.tool()
    async def update_clinical_group(
        group_id: str,
        ctx: Context,
        name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Update Clinical group attributes.

        Args:
            group_id: Group UUID.
            name: Optional new name.
            description: Optional new description.
        """
        body: dict[str, str] = {}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        if not body:
            raise ValueError("provide name and/or description to update")
        async with viya_session("update_clinical_group", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_GROUPS}/{group_id}",
                body=body,
            )

    @mcp.tool()
    async def update_clinical_group_members(
        group_id: str,
        ctx: Context,
        add_members: PrincipalList | None = None,
        remove_members: PrincipalList | None = None,
    ) -> dict[str, Any]:
        """Add or remove members on a Clinical group.

        Args:
            group_id: Group UUID.
            add_members: Principals to add (``{id, typeId}``).
            remove_members: Principals to remove.
        """
        body = access.members_update_body(add_members, remove_members)
        async with viya_session("update_clinical_group_members", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_GROUPS}/{group_id}",
                patch_url=f"{_GROUPS}/{group_id}/members",
                params={"action": "UPDATE_MEMBERS"},
                body=body,
                content_type="application/vnd.sas.clinical.members.update+json",
            )

    @mcp.tool()
    async def delete_clinical_group(group_id: str, ctx: Context) -> dict[str, Any]:
        """Delete a Clinical group.

        Args:
            group_id: Group UUID.
        """
        async with viya_session("delete_clinical_group", ctx) as client:
            resp = await client.delete(
                f"{VIYA_ENDPOINT}{_GROUPS}/{group_id}",
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return {"status": "deleted", "group_id": group_id}

    # --- roles & privileges --------------------------------------------------

    @mcp.tool()
    async def list_clinical_roles(
        context_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List roles assigned at a Clinical context.

        Args:
            context_id: Context UUID.
            limit: Maximum roles (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_roles", ctx) as client:
            items, _ = await get_paged_items(
                _ROLES,
                client,
                limit=limit,
                filters=filter_expr,
                extra_params={"contextId": context_id},
            )
            return return_items(items, _ROLE_FIELDS) if items else []

    @mcp.tool()
    async def get_clinical_role(role_id: str, ctx: Context) -> dict[str, Any]:
        """Get one Clinical role by id.

        Args:
            role_id: Role UUID.
        """
        async with viya_session("get_clinical_role", ctx) as client:
            return await get_json(f"{_ROLES}/{role_id}", client)

    @mcp.tool()
    async def list_clinical_unassigned_roles(
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List roles that are not yet assigned to a context.

        Args:
            limit: Maximum roles (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_unassigned_roles", ctx) as client:
            items, _ = await get_paged_items(
                f"{_ROLES}/unassigned",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _ROLE_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_role_members(
        role_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List members of a Clinical role.

        Args:
            role_id: Role UUID.
            limit: Maximum members (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_role_members", ctx) as client:
            items, _ = await get_paged_items(
                f"{_ROLES}/{role_id}/members",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_role_member_candidates(
        role_id: str,
        ctx: Context,
        limit: int = 100,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """List candidates that can be assigned to a Clinical role.

        Args:
            role_id: Role UUID.
            limit: Maximum candidates (default 100).
            filter_expr: Optional Viya filter expression.
        """
        async with viya_session("list_clinical_role_member_candidates", ctx) as client:
            items, _ = await get_paged_items(
                f"{_ROLES}/{role_id}/members/candidates",
                client,
                limit=limit,
                filters=filter_expr,
            )
            return return_items(items, _PRINCIPAL_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_role_privileges(
        role_id: str,
        ctx: Context,
    ) -> list[dict[str, Any]]:
        """List privileges granted to a Clinical role.

        Args:
            role_id: Role UUID.
        """
        async with viya_session("list_clinical_role_privileges", ctx) as client:
            data = await get_json(f"{_ROLES}/{role_id}/privileges", client)
            items = data.get("items", []) or []
            return return_items(items, _PRIVILEGE_FIELDS) if items else []

    @mcp.tool()
    async def list_clinical_privileges(ctx: Context) -> list[dict[str, Any]]:
        """List all privileges that can be assigned to Clinical roles."""
        async with viya_session("list_clinical_privileges", ctx) as client:
            data = await get_json(f"{_ROLES}/privileges", client)
            items = data.get("items", []) or []
            return return_items(items, _PRIVILEGE_FIELDS) if items else []

    @mcp.tool()
    async def create_clinical_role(
        context_id: str,
        name: str,
        ctx: Context,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a role on a Clinical context.

        Args:
            context_id: Context UUID.
            name: Role name.
            description: Optional description.
        """
        params: dict[str, Any] = {"contextId": context_id, "name": name}
        if description:
            params["description"] = description
        async with viya_session("create_clinical_role", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_ROLES}",
                params=params,
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "created", "name": name}

    @mcp.tool()
    async def inherit_clinical_roles(
        context_id: str,
        role_ids: StringList,
        ctx: Context,
    ) -> dict[str, Any]:
        """Inherit one or more parent-context roles into a child context.

        Args:
            context_id: Target context UUID.
            role_ids: Role UUIDs from the parent context to inherit.
        """
        if not role_ids:
            raise ValueError("role_ids must not be empty")
        async with viya_session("inherit_clinical_roles", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}{_ROLES}/inherited",
                params={"contextId": context_id},
                json=role_ids,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "ok", "role_ids": role_ids}

    @mcp.tool()
    async def update_clinical_role(
        role_id: str,
        ctx: Context,
        name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Update Clinical role attributes.

        Args:
            role_id: Role UUID.
            name: Optional new name.
            description: Optional new description.
        """
        body: dict[str, str] = {}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        if not body:
            raise ValueError("provide name and/or description to update")
        async with viya_session("update_clinical_role", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_ROLES}/{role_id}",
                body=body,
            )

    @mcp.tool()
    async def update_clinical_role_members(
        role_id: str,
        ctx: Context,
        add_members: PrincipalList | None = None,
        remove_members: PrincipalList | None = None,
    ) -> dict[str, Any]:
        """Add or remove members on a Clinical role.

        Args:
            role_id: Role UUID.
            add_members: Principals to add (``{id, typeId}``).
            remove_members: Principals to remove.
        """
        body = access.members_update_body(add_members, remove_members)
        async with viya_session("update_clinical_role_members", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_ROLES}/{role_id}",
                patch_url=f"{_ROLES}/{role_id}/members",
                params={"action": "UPDATE_MEMBERS"},
                body=body,
                content_type="application/vnd.sas.clinical.members.update+json",
            )

    @mcp.tool()
    async def update_clinical_role_privileges(
        role_id: str,
        ctx: Context,
        add_privileges: StringList | None = None,
        remove_privileges: StringList | None = None,
    ) -> dict[str, Any]:
        """Add or remove privileges on a Clinical role.

        Args:
            role_id: Role UUID.
            add_privileges: Privilege ids to add (e.g. ``PRIVILEGE_MANAGE_MEMBERSHIP``).
            remove_privileges: Privilege ids to remove.
        """
        body: dict[str, Any] = {"version": 1}
        if add_privileges:
            body["addPrivileges"] = list(add_privileges)
        if remove_privileges:
            body["removePrivileges"] = list(remove_privileges)
        if "addPrivileges" not in body and "removePrivileges" not in body:
            raise ValueError("provide add_privileges and/or remove_privileges")
        async with viya_session("update_clinical_role_privileges", ctx) as client:
            return await access.patch_json_with_etag(
                client,
                resource_url=f"{_ROLES}/{role_id}",
                patch_url=f"{_ROLES}/{role_id}/privileges",
                body=body,
                content_type="application/vnd.sas.clinical.privileges.update+json",
            )

    @mcp.tool()
    async def delete_clinical_role(role_id: str, ctx: Context) -> dict[str, Any]:
        """Delete a Clinical role.

        Args:
            role_id: Role UUID.
        """
        async with viya_session("delete_clinical_role", ctx) as client:
            resp = await client.delete(
                f"{VIYA_ENDPOINT}{_ROLES}/{role_id}",
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return {"status": "deleted", "role_id": role_id}

    # --- item permissions / owner --------------------------------------------

    @mcp.tool()
    async def get_clinical_item_permissions(
        item_id: str,
        ctx: Context,
        current: bool = True,
    ) -> dict[str, Any]:
        """Get permissions for a Clinical repository item (context/folder/file).

        Args:
            item_id: Item UUID.
            current: True for current permissions (default); false for defaults.
        """
        async with viya_session("get_clinical_item_permissions", ctx) as client:
            return await get_json(
                f"{_REPO_ITEMS}/{item_id}/permissions",
                client,
                params={"current": str(current).lower()},
            )

    @mcp.tool()
    async def update_clinical_item_permissions(
        item_id: str,
        ctx: Context,
        additions: PermissionEntryList | None = None,
        updates: PermissionEntryList | None = None,
        removals: PrincipalList | None = None,
        current: bool = True,
    ) -> dict[str, Any]:
        """Add, update, or remove permission entries on a Clinical item.

        Args:
            item_id: Item UUID.
            additions: Permission entries to add (principal + read/write/delete/admin).
            updates: Existing entries to update.
            removals: Principals to remove (``{id, typeId}``).
            current: Update current permissions (default true).
        """
        body: dict[str, Any] = {"version": 1}
        if additions:
            body["additions"] = additions
        if updates:
            body["updates"] = updates
        if removals:
            body["removals"] = access.normalize_principals(removals)
        if len(body) == 1:
            raise ValueError("provide at least one of additions, updates, or removals")
        async with viya_session("update_clinical_item_permissions", ctx) as client:
            resp = await client.patch(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{item_id}/permissions",
                params={"current": str(current).lower()},
                json=body,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/vnd.sas.clinical.permissions.update+json",
                },
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "ok", "item_id": item_id}

    @mcp.tool()
    async def get_clinical_item_owner(
        item_id: str,
        ctx: Context,
        current: bool = True,
    ) -> dict[str, Any]:
        """Get the owner of a Clinical repository item.

        Args:
            item_id: Item UUID.
            current: True for current owner (default).
        """
        async with viya_session("get_clinical_item_owner", ctx) as client:
            return await get_json(
                f"{_REPO_ITEMS}/{item_id}/owner",
                client,
                params={"current": str(current).lower()},
            )

    @mcp.tool()
    async def set_clinical_item_owner(
        item_id: str,
        owner_id: str,
        ctx: Context,
        current: bool = True,
    ) -> dict[str, Any]:
        """Set the owner of a Clinical repository item.

        Args:
            item_id: Item UUID.
            owner_id: New owner user id.
            current: Update current owner (default true).
        """
        async with viya_session("set_clinical_item_owner", ctx) as client:
            resp = await client.put(
                f"{VIYA_ENDPOINT}{_REPO_ITEMS}/{item_id}/owner",
                params={"ownerId": owner_id, "current": str(current).lower()},
                headers={"Accept": "application/json"},
            )
            raise_for_viya_status(resp)
            return resp.json() if resp.content else {"status": "ok", "owner_id": owner_id}

    # --- composite export / import / copy ------------------------------------

    @mcp.tool()
    async def export_clinical_access_model(
        context_id: str,
        ctx: Context,
        include_membership: bool = True,
        include_groups: bool = True,
        include_roles: bool = True,
        include_folder_permissions: bool = True,
        folder_limit: int = 200,
    ) -> dict[str, Any]:
        """Export membership, groups, roles/privileges, and optional folder ACLs.

        Args:
            context_id: Source context UUID.
            include_membership: Include context members.
            include_groups: Include groups and their members.
            include_roles: Include roles, members, and privileges.
            include_folder_permissions: Include permissions on the context and folders.
            folder_limit: Max descendant folders to scan for ACLs.
        """
        async with viya_session("export_clinical_access_model", ctx) as client:
            return await access.export_access_model(
                client,
                context_id,
                include_membership=include_membership,
                include_groups=include_groups,
                include_roles=include_roles,
                include_folder_permissions=include_folder_permissions,
                folder_limit=folder_limit,
            )

    @mcp.tool()
    async def import_clinical_access_model(
        target_context_id: str,
        model: AccessModelParam,
        ctx: Context,
        dry_run: bool = False,
        include_membership: bool = True,
        include_groups: bool = True,
        include_roles: bool = True,
        include_folder_permissions: bool = True,
    ) -> dict[str, Any]:
        """Import an exported Clinical access model onto a target context.

        Creates missing groups/roles by name, assigns members and privileges,
        then optionally applies folder permissions by relative path.

        Args:
            target_context_id: Destination context UUID.
            model: Object from ``export_clinical_access_model``.
            dry_run: When true, only return the planned actions.
            include_membership: Apply context members.
            include_groups: Create/update groups.
            include_roles: Create/update roles and privileges.
            include_folder_permissions: Apply folder ACLs where paths match.
        """
        async with viya_session("import_clinical_access_model", ctx) as client:
            return await access.import_access_model(
                client,
                target_context_id,
                model,
                dry_run=dry_run,
                include_membership=include_membership,
                include_groups=include_groups,
                include_roles=include_roles,
                include_folder_permissions=include_folder_permissions,
            )

    @mcp.tool()
    async def copy_clinical_access_model(
        source_context_id: str,
        target_context_id: str,
        ctx: Context,
        dry_run: bool = False,
        include_membership: bool = True,
        include_groups: bool = True,
        include_roles: bool = True,
        include_folder_permissions: bool = True,
    ) -> dict[str, Any]:
        """Copy access model from one Clinical context to another (export + import).

        Args:
            source_context_id: Source context UUID.
            target_context_id: Destination context UUID.
            dry_run: When true, only plan the import.
            include_membership: Copy context members.
            include_groups: Copy groups.
            include_roles: Copy roles and privileges.
            include_folder_permissions: Copy folder ACLs where paths match.
        """
        async with viya_session("copy_clinical_access_model", ctx) as client:
            return await access.copy_access_model(
                client,
                source_context_id,
                target_context_id,
                dry_run=dry_run,
                include_membership=include_membership,
                include_groups=include_groups,
                include_roles=include_roles,
                include_folder_permissions=include_folder_permissions,
            )
