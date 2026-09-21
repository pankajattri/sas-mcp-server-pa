# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Helpers for Clinical Repository membership, groups, roles, and permissions."""

from __future__ import annotations

from typing import Any

import httpx

from sas_mcp_server.config import VIYA_ENDPOINT
from sas_mcp_server.viya_client import (
    get_json,
    get_paged_items,
    raise_for_viya_status,
)

_REPO = "/clinicalRepository"
_MEMBERSHIPS = f"{_REPO}/memberships"
_GROUPS = f"{_REPO}/groups"
_ROLES = f"{_REPO}/roles"
_ITEMS = f"{_REPO}/repository/items"


def normalize_principals(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Normalize MCP principal dicts to Viya ``principalIdentity`` objects."""
    if not raw:
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("each principal must be an object with id and typeId")
        pid = item.get("id")
        type_id = item.get("typeId") or item.get("type_id")
        if not pid or not type_id:
            raise ValueError("each principal needs id and typeId (user|group)")
        out.append({"version": 1, "id": str(pid), "typeId": str(type_id)})
    return out


def members_update_body(
    add_members: list[dict[str, Any]] | None = None,
    remove_members: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"version": 1}
    adds = normalize_principals(add_members)
    removes = normalize_principals(remove_members)
    if adds:
        body["addMembers"] = adds
    if removes:
        body["removeMembers"] = removes
    if "addMembers" not in body and "removeMembers" not in body:
        raise ValueError("provide at least one of add_members or remove_members")
    return body


async def get_etag(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> str:
    resp = await client.get(
        f"{VIYA_ENDPOINT}{url}",
        params=params or {},
        headers={"Accept": "application/json"},
    )
    raise_for_viya_status(resp)
    return resp.headers.get("etag", "")


async def patch_json_with_etag(
    client: httpx.AsyncClient,
    *,
    resource_url: str,
    patch_url: str | None = None,
    body: Any,
    params: dict[str, Any] | None = None,
    resource_params: dict[str, Any] | None = None,
    content_type: str = "application/json",
) -> dict[str, Any]:
    """PATCH with If-Match from a prior GET of *resource_url*."""
    etag = await get_etag(client, resource_url, resource_params)
    target = patch_url or resource_url
    resp = await client.patch(
        f"{VIYA_ENDPOINT}{target}",
        params=params or {},
        json=body,
        headers={
            "Accept": "application/json",
            "Content-Type": content_type,
            "If-Match": etag,
        },
    )
    raise_for_viya_status(resp)
    if not resp.content:
        return {"status": "ok"}
    return resp.json()


async def list_collection(
    client: httpx.AsyncClient,
    url: str,
    *,
    limit: int = 100,
    filters: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    items, _ = await get_paged_items(
        url, client, limit=limit, filters=filters, extra_params=extra_params
    )
    return items


def _principal_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id", ""),
        "typeId": item.get("typeId", ""),
        "name": item.get("name", ""),
        "displayName": item.get("displayName", ""),
    }


def _role_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id", ""),
        "name": item.get("name", ""),
        "displayName": item.get("displayName", ""),
        "description": item.get("description", ""),
        "inherited": item.get("inherited", False),
        "definedContextId": item.get("definedContextId", ""),
        "assignedContextId": item.get("assignedContextId", ""),
    }


def _group_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id", ""),
        "name": item.get("name", ""),
        "description": item.get("description", ""),
        "displayName": item.get("displayName", ""),
    }


def _privilege_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id", ""),
        "name": item.get("name", ""),
        "description": item.get("description", ""),
        "typeId": item.get("typeId", ""),
    }


async def export_access_model(
    client: httpx.AsyncClient,
    context_id: str,
    *,
    include_membership: bool = True,
    include_groups: bool = True,
    include_roles: bool = True,
    include_folder_permissions: bool = True,
    folder_limit: int = 200,
) -> dict[str, Any]:
    """Snapshot membership/groups/roles(/privileges) and optional folder ACLs."""
    model: dict[str, Any] = {
        "version": 1,
        "context_id": context_id,
        "membership": None,
        "members": [],
        "groups": [],
        "roles": [],
        "folder_permissions": [],
    }

    if include_membership:
        model["membership"] = await get_json(f"{_MEMBERSHIPS}/{context_id}", client)
        members, _ = await get_paged_items(
            f"{_MEMBERSHIPS}/{context_id}/members",
            client,
            limit=500,
            extra_params={"assignedOnly": "true"},
        )
        model["members"] = [_principal_summary(m) for m in members]

    if include_groups:
        groups, _ = await get_paged_items(
            _GROUPS, client, limit=200, extra_params={"contextId": context_id}
        )
        for group in groups:
            gid = group.get("id", "")
            g_members, _ = await get_paged_items(
                f"{_GROUPS}/{gid}/members", client, limit=500
            )
            entry = _group_summary(group)
            entry["members"] = [_principal_summary(m) for m in g_members]
            model["groups"].append(entry)

    if include_roles:
        roles, _ = await get_paged_items(
            _ROLES, client, limit=200, extra_params={"contextId": context_id}
        )
        for role in roles:
            rid = role.get("id", "")
            r_members, _ = await get_paged_items(
                f"{_ROLES}/{rid}/members", client, limit=500
            )
            priv_data = await get_json(f"{_ROLES}/{rid}/privileges", client)
            privileges = priv_data.get("items", []) or []
            entry = _role_summary(role)
            entry["members"] = [_principal_summary(m) for m in r_members]
            entry["privileges"] = [_privilege_summary(p) for p in privileges]
            model["roles"].append(entry)

    if include_folder_permissions:
        children, _ = await get_paged_items(
            f"{_ITEMS}/{context_id}/children",
            client,
            limit=folder_limit,
            extra_params={"recurse": "true"},
        )
        # Include the context itself, then folders under it.
        targets = [{"id": context_id, "path": "", "primaryType": "CONTEXT"}]
        targets.extend(
            c for c in children if (c.get("primaryType") or "").upper() in {"FOLDER", "CONTEXT"}
        )
        for item in targets[: folder_limit + 1]:
            item_id = item.get("id", "")
            if not item_id:
                continue
            try:
                perms = await get_json(
                    f"{_ITEMS}/{item_id}/permissions",
                    client,
                    params={"current": "true"},
                )
            except httpx.HTTPStatusError:
                continue
            model["folder_permissions"].append(
                {
                    "item_id": item_id,
                    "path": item.get("path", ""),
                    "name": item.get("name", ""),
                    "primaryType": item.get("primaryType", ""),
                    "permissions": perms,
                }
            )

    return model


async def import_access_model(
    client: httpx.AsyncClient,
    target_context_id: str,
    model: dict[str, Any],
    *,
    dry_run: bool = False,
    include_membership: bool = True,
    include_groups: bool = True,
    include_roles: bool = True,
    include_folder_permissions: bool = True,
) -> dict[str, Any]:
    """Apply an exported access model onto *target_context_id*."""
    plan: list[str] = []
    results: dict[str, Any] = {
        "status": "dry_run" if dry_run else "ok",
        "target_context_id": target_context_id,
        "plan": plan,
        "created_groups": {},
        "created_roles": {},
        "applied": [],
    }

    # --- groups (create by name, then members) --------------------------------
    group_id_by_name: dict[str, str] = {}
    if include_groups:
        existing_groups, _ = await get_paged_items(
            _GROUPS, client, limit=200, extra_params={"contextId": target_context_id}
        )
        group_id_by_name = {
            (g.get("name") or ""): g.get("id", "") for g in existing_groups if g.get("name")
        }
        for group in model.get("groups") or []:
            name = group.get("name") or ""
            if not name:
                continue
            if name in group_id_by_name:
                plan.append(f"reuse group '{name}'")
            else:
                plan.append(f"create group '{name}'")
                if not dry_run:
                    resp = await client.post(
                        f"{VIYA_ENDPOINT}{_GROUPS}",
                        params={
                            "contextId": target_context_id,
                            "name": name,
                            **(
                                {"description": group["description"]}
                                if group.get("description")
                                else {}
                            ),
                        },
                        headers={"Accept": "application/json"},
                    )
                    raise_for_viya_status(resp)
                    created = resp.json() if resp.content else {}
                    gid = created.get("id", "")
                    group_id_by_name[name] = gid
                    results["created_groups"][name] = gid
            gid = group_id_by_name.get(name, "")
            members = group.get("members") or []
            if gid and members:
                plan.append(f"set {len(members)} member(s) on group '{name}'")
                if not dry_run:
                    await patch_json_with_etag(
                        client,
                        resource_url=f"{_GROUPS}/{gid}",
                        patch_url=f"{_GROUPS}/{gid}/members",
                        params={"action": "UPDATE_MEMBERS"},
                        body=members_update_body(add_members=members),
                    )
                    results["applied"].append(f"group_members:{name}")

    # --- roles (create by name, then members + privileges) --------------------
    role_id_by_name: dict[str, str] = {}
    if include_roles:
        existing_roles, _ = await get_paged_items(
            _ROLES, client, limit=200, extra_params={"contextId": target_context_id}
        )
        role_id_by_name = {
            (r.get("name") or ""): r.get("id", "") for r in existing_roles if r.get("name")
        }
        for role in model.get("roles") or []:
            name = role.get("name") or ""
            if not name:
                continue
            if name in role_id_by_name:
                plan.append(f"reuse role '{name}'")
            else:
                plan.append(f"create role '{name}'")
                if not dry_run:
                    params: dict[str, Any] = {
                        "contextId": target_context_id,
                        "name": name,
                    }
                    if role.get("description"):
                        params["description"] = role["description"]
                    resp = await client.post(
                        f"{VIYA_ENDPOINT}{_ROLES}",
                        params=params,
                        headers={"Accept": "application/json"},
                    )
                    raise_for_viya_status(resp)
                    created = resp.json() if resp.content else {}
                    rid = created.get("id", "")
                    role_id_by_name[name] = rid
                    results["created_roles"][name] = rid
            rid = role_id_by_name.get(name, "")
            members = role.get("members") or []
            if rid and members:
                plan.append(f"set {len(members)} member(s) on role '{name}'")
                if not dry_run:
                    await patch_json_with_etag(
                        client,
                        resource_url=f"{_ROLES}/{rid}",
                        patch_url=f"{_ROLES}/{rid}/members",
                        params={"action": "UPDATE_MEMBERS"},
                        body=members_update_body(add_members=members),
                    )
                    results["applied"].append(f"role_members:{name}")
            privileges = role.get("privileges") or []
            privilege_ids = [p.get("id") for p in privileges if p.get("id")]
            if rid and privilege_ids:
                plan.append(f"set {len(privilege_ids)} privilege(s) on role '{name}'")
                if not dry_run:
                    await patch_json_with_etag(
                        client,
                        resource_url=f"{_ROLES}/{rid}",
                        patch_url=f"{_ROLES}/{rid}/privileges",
                        body={"version": 1, "addPrivileges": privilege_ids},
                    )
                    results["applied"].append(f"role_privileges:{name}")

    # --- context membership ---------------------------------------------------
    if include_membership:
        members = model.get("members") or []
        # Prefer assigning users and global/source groups. Context-local groups
        # are remapped by name when possible.
        remapped: list[dict[str, Any]] = []
        for member in members:
            type_id = (member.get("typeId") or "").lower()
            if type_id == "group":
                # If this was a source-local group, try target group of same name.
                name = member.get("name") or ""
                if name and name in group_id_by_name:
                    remapped.append({"id": group_id_by_name[name], "typeId": "group"})
                else:
                    remapped.append({"id": member.get("id"), "typeId": "group"})
            else:
                remapped.append({"id": member.get("id"), "typeId": member.get("typeId") or "user"})
        remapped = [m for m in remapped if m.get("id")]
        if remapped:
            plan.append(f"add {len(remapped)} context member(s)")
            if not dry_run:
                await patch_json_with_etag(
                    client,
                    resource_url=f"{_MEMBERSHIPS}/{target_context_id}",
                    patch_url=f"{_MEMBERSHIPS}/{target_context_id}/members",
                    params={"action": "UPDATE_MEMBERS"},
                    body=members_update_body(add_members=remapped),
                    content_type="application/vnd.sas.clinical.members.update+json",
                )
                results["applied"].append("context_members")

    # --- folder permissions by relative path ---------------------------------
    if include_folder_permissions:
        source_folders = model.get("folder_permissions") or []
        target_children, _ = await get_paged_items(
            f"{_ITEMS}/{target_context_id}/children",
            client,
            limit=500,
            extra_params={"recurse": "true"},
        )
        target_by_rel: dict[str, str] = {"": target_context_id}
        # Build relative paths under source context from stored paths when possible.
        source_root = ""
        for entry in source_folders:
            if entry.get("item_id") == model.get("context_id"):
                source_root = entry.get("path") or ""
                break

        def _rel(path: str) -> str:
            if not path:
                return ""
            if source_root and path.startswith(source_root):
                rest = path[len(source_root) :]
                return rest[1:] if rest.startswith("/") else rest
            return path

        for child in target_children:
            path = child.get("path") or ""
            # Store by basename path suffix for matching when roots differ.
            target_by_rel[path] = child.get("id", "")
            target_by_rel[child.get("name", "")] = child.get("id", "")

        for entry in source_folders:
            perms = entry.get("permissions") or {}
            entries = perms.get("entries") or []
            if not entries:
                continue
            rel = _rel(entry.get("path") or "")
            target_id = target_by_rel.get(rel) or target_by_rel.get(entry.get("name") or "")
            if not target_id and entry.get("item_id") == model.get("context_id"):
                target_id = target_context_id
            if not target_id:
                plan.append(
                    f"skip permissions for '{entry.get('path') or entry.get('name')}' (no target match)"
                )
                continue
            additions = []
            for e in entries:
                principal = e.get("principal") or {}
                pid = principal.get("id")
                type_id = principal.get("typeId")
                if not pid or not type_id:
                    continue
                # Remap local groups by name when present on the entry principal.
                if str(type_id).lower() == "group" and principal.get("name") in group_id_by_name:
                    pid = group_id_by_name[principal["name"]]
                additions.append(
                    {
                        "version": 1,
                        "principal": {"version": 1, "id": pid, "typeId": type_id},
                        "readPermission": e.get("readPermission", "UNSET"),
                        "writePermission": e.get("writePermission", "UNSET"),
                        "deletePermission": e.get("deletePermission", "UNSET"),
                        "adminPermission": e.get("adminPermission", "UNSET"),
                    }
                )
            if not additions:
                continue
            plan.append(f"apply {len(additions)} permission entr(y/ies) on '{rel or '/'}'")
            if not dry_run:
                resp = await client.patch(
                    f"{VIYA_ENDPOINT}{_ITEMS}/{target_id}/permissions",
                    params={"current": "true"},
                    json={"version": 1, "additions": additions},
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/vnd.sas.clinical.permissions.update+json",
                    },
                )
                raise_for_viya_status(resp)
                results["applied"].append(f"permissions:{rel or '/'}")

    results["plan"] = plan
    return results


async def copy_access_model(
    client: httpx.AsyncClient,
    source_context_id: str,
    target_context_id: str,
    *,
    dry_run: bool = False,
    include_membership: bool = True,
    include_groups: bool = True,
    include_roles: bool = True,
    include_folder_permissions: bool = True,
) -> dict[str, Any]:
    model = await export_access_model(
        client,
        source_context_id,
        include_membership=include_membership,
        include_groups=include_groups,
        include_roles=include_roles,
        include_folder_permissions=include_folder_permissions,
    )
    applied = await import_access_model(
        client,
        target_context_id,
        model,
        dry_run=dry_run,
        include_membership=include_membership,
        include_groups=include_groups,
        include_roles=include_roles,
        include_folder_permissions=include_folder_permissions,
    )
    return {"source": model, "import": applied}
