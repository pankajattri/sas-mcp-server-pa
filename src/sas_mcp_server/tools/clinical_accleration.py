# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tier 10 — Clinical Acceleration tools (SAS Clinical Acceleration Repository)."""

from collections.abc import Awaitable, Callable
from typing import Any

from fastmcp import Context, FastMCP

from ..viya_client import filter_literal, get_paged_items, return_items
from ._common import make_session_helpers

_REPO_ITEMS = "/clinicalRepository/repository/items"
_PRIMARY_TYPES = frozenset({"FILE", "FOLDER", "CONTEXT"})


def _and_filter(clauses: list[str]) -> str | None:
    """Combine Viya filter clauses with ``and(...)``, or return the sole clause."""
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return f"and({','.join(clauses)})"


def _normalize_context_path(context_path: str) -> str:
    """Normalize a repository path for ``startsWith(path, ...)`` filtering."""
    path = context_path.strip()
    if not path:
        return ""
    if not path.startswith("/"):
        path = f"/{path}"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return path


def _item_type_clause(item_type: str) -> str:
    """Map *item_type* to a ``primaryType`` or ``typeId`` filter clause."""
    raw = item_type.strip()
    upper = raw.upper()
    if upper in _PRIMARY_TYPES:
        return f"eq(primaryType,'{upper}')"
    return f"eq(typeId,'{filter_literal(raw)}')"


def _query_clause(query: str, *, search_content: bool) -> str | None:
    """Build the name / description filter for *query*.

    ``search_content=True`` also matches item ``description`` metadata. The
    Clinical Repository list API does not expose full-text file-body search as a
    result listing — that ``text`` field exists only on the ZIP download
    endpoint (``POST /repository/files/content``).
    """
    text = query.strip()
    if not text:
        return None
    name = f"contains(name,'{filter_literal(text)}')"
    if not search_content:
        return name
    description = f"contains(description,'{filter_literal(text)}')"
    return f"or({name},{description})"


def register(mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]) -> None:
    """Register Tier 10 (Clinical Acceleration) tools on *mcp*."""

    viya_session, _ = make_session_helpers(get_token)

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

        Calls ``GET /clinicalRepository/repository/items`` with a Viya filter built
        from the arguments (see the Clinical Repository REST API).

        Args:
            query: Substring matched against item names (and descriptions when
                ``search_content`` is true). Empty string lists items under
                ``context_path`` / ``item_type`` without a name filter.
            context_path: Optional repository path to search under
                (e.g. ``/StudyA/Data``). Matched with ``startsWith(path, ...)``.
            item_type: Optional type filter. Use ``FILE``, ``FOLDER``, or
                ``CONTEXT`` for primary types, or a Clinical type id such as
                ``sasdataset`` / ``folder`` / ``businessunit``.
            search_content: When true, also match item description metadata.
                Full-text search of file bodies is not available on the list
                API (only on the ZIP download endpoint).
            limit: Maximum items to return (default 50).
        """
        clauses: list[str] = []
        query_part = _query_clause(query, search_content=search_content)
        if query_part:
            clauses.append(query_part)
        if context_path and context_path.strip():
            path = _normalize_context_path(context_path)
            clauses.append(f"startsWith(path,'{filter_literal(path)}')")
        if item_type and item_type.strip():
            clauses.append(_item_type_clause(item_type))

        filters = _and_filter(clauses)
        async with viya_session("search_clinical_repository", ctx) as client:
            items, _ = await get_paged_items(
                _REPO_ITEMS,
                client,
                limit=limit,
                filters=filters,
                extra_params={"sortBy": "name:ascending:primary"},
            )
            if not items:
                return []
            return return_items(
                items,
                [
                    "id",
                    "name",
                    "primaryType",
                    "typeId",
                    "path",
                    "location",
                    "size",
                    "state",
                    "description",
                ],
            )
