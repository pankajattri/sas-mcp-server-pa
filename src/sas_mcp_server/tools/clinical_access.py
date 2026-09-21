# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Clinical Acceleration access-control tools (membership, groups, roles, ACLs)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from fastmcp import Context, FastMCP
from pydantic import BeforeValidator

from ..config import VIYA_ENDPOINT
from ..helpers import clinical_access_helpers as access
from ..viya_client import get_json, get_paged_items, raise_for_viya_status, return_items
from ._common import coerce_json_dict, coerce_json_list, make_session_helpers

PrincipalList = Annotated[list[dict[str, Any]], BeforeValidator(coerce_json_list)]
PermissionEntryList = Annotated[list[dict[str, Any]], BeforeValidator(coerce_json_list)]
StringList = Annotated[list[str], BeforeValidator(coerce_json_list)]
AccessModelParam = Annotated[dict[str, Any], BeforeValidator(coerce_json_dict)]

_MEMBERSHIPS = "/clinicalRepository/memberships"
_GROUPS = "/clinicalRepository/groups"
_ROLES = "/clinicalRepository/roles"
_ITEMS = "/clinicalRepository/repository/items"

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


def register(mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]) -> None:
    """Register membership / groups / roles / permissions tools on *mcp*."""

    viya_session, _ = make_session_helpers(get_token)

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
                f"{_ITEMS}/{item_id}/permissions",
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
                f"{VIYA_ENDPOINT}{_ITEMS}/{item_id}/permissions",
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
                f"{_ITEMS}/{item_id}/owner",
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
                f"{VIYA_ENDPOINT}{_ITEMS}/{item_id}/owner",
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
