#!/usr/bin/env python3
"""
Async client for executable (tool) syscalls.
"""

from typing import Any

from .._.main import call
from ALaDOS.src.python.utils.sr_edit import SearchAndReplaceBlock # pyright: ignore


async def execute(
    slave_addr: int,
    id: int | str,
    timeout: int = 10,
    kwargs: dict[str, Any] | None = None,
) -> str:
    """
    Executes a tool (non‑builtin) from the database by ID.
    Returns the tool’s stdout output.
    """
    result = await call(
        "tool_execute",
        slave_addr,
        {"id": id, "timeout": timeout, "kwargs": kwargs},
    )
    return result


async def create(
    slave_addr: int,
    description: str,
    header: str,
    body: str,
    name: str | None = None,
) -> int:
    """
    Creates a new Python tool in the database.
    Returns the new tool's address.
    """
    result = await call(
        "tool_create",
        slave_addr,
        {
            "description": description,
            "header": header,
            "body": body,
            "name": name,
        },
    )
    return int(result)


async def edit(
    slave_addr: int,
    id: int | str,
    header_change: SearchAndReplaceBlock | None = None,
    body_change: SearchAndReplaceBlock | None = None,
    new_description: str | None = None,
) -> None:
    """
    Edits an existing tool. At least one change must be provided.
    Returns nothing.
    """
    await call(
        "tool_edit",
        slave_addr,
        {
            "id": id,
            "header_change": header_change,
            "body_change": body_change,
            "new_description": new_description,
        },
    )
