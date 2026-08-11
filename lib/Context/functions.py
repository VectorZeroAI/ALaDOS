#!/usr/bin/env python3
"""
Async client for context‑related syscalls.
"""

import json

from .._.main import call


async def add(slave_addr: int, id: int | str) -> None:
    """
    Adds an item (knowledge, tool, etc.) to the master’s context.
    Returns nothing.
    """
    await call("context_add", slave_addr, {"id": id})


async def window_semantic_land(slave_addr: int, query: str) -> int:
    """
    Lands the viewing window on the most semantically similar item.
    Returns the new anchor address.
    """
    result = await call(
        "context_window_semantic_land",
        slave_addr,
        {"query": query},
    )
    return int(result)


async def window_land_by_addr(slave_addr: int, id: int | str) -> None:
    """
    Lands the viewing window directly on a specific item.
    Returns nothing.
    """
    await call("context_window_land_by_addr", slave_addr, {"id": id})


async def window_change_size(
    slave_addr: int,
    left: int = 0,
    right: int = 0,
) -> dict[str, int]:
    """
    Changes the left/right size of the viewing window.
    Returns a dict with new 'left' and 'right' sizes.
    """
    result = await call(
        "context_window_change_size",
        slave_addr,
        {"left": left, "right": right},
    )
    return json.loads(result)


async def window_move_anchor(slave_addr: int, amount: int) -> int:
    """
    Moves the viewing window anchor by `amount` (negative = left).
    Returns the new anchor address.
    """
    result = await call(
        "context_window_move_anchor",
        slave_addr,
        {"amount": amount},
    )
    return int(result)


async def unload_item(slave_addr: int, id: int | str) -> None:
    """
    Removes an item from the context.
    Returns nothing.
    """
    await call("context_unload_item", slave_addr, {"id": id})
