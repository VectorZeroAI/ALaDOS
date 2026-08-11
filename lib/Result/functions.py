#!/usr/bin/env python3
"""
Async client for result‑writing syscalls.
"""

from .._.main import call


async def add_master_result(slave_addr: int, text: str) -> None:
    """
    Appends `text` to the current master result.
    Returns nothing.
    """
    await call("result_add_master_result", slave_addr, {"text": text})


async def write(slave_addr: int, text: str) -> str:
    """
    Writes `text` as the result of the current slave instruction.
    Returns the same text (it is passed through).
    """
    return await call("result_write", slave_addr, {"text": text})
