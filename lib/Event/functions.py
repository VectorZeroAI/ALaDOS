#!/usr/bin/env python3
"""
Async client for event‑reaction syscalls.
"""

import json

from .._.main import call

from ALaDOS.src.python.executor.types import SlaveScope # pyright: ignore


async def register_reaction_rmt(
    slave_addr: int,
    event_path: str,
    rmt_id: int | str,
    args: dict[str, str],
) -> int:
    """
    Registers an RMT as a callback for a NATS event.
    Returns the consumer address.
    """
    result = await call(
        "event_register_reaction_rmt",
        slave_addr,
        {"event_path": event_path, "rmt_id": rmt_id, "args": args},
    )
    return int(result)


async def register_reaction_slave(
    slave_addr: int,
    event_path: str,
    instruction: str,
    scope: SlaveScope = "general",
) -> int:
    """
    Registers a single slave as a callback for a NATS event.
    The instruction may contain ${{data}} and ${{subject}} placeholders.
    Returns the consumer address.
    """
    result = await call(
        "event_register_reaction_slave",
        slave_addr,
        {"event_path": event_path, "instruction": instruction, "scope": scope},
    )
    return int(result)


async def create_result(
    slave_addr: int,
    event_path: str,
    result_str: str,
    name: str | None = None,
) -> dict[str, int]:
    """
    Creates a result that will be filled when the event fires.
    The result string may contain ${{data}} and ${{event}}.
    Returns a dict with 'result_addr' and 'consumer_addr'.
    """
    result = await call(
        "event_create_result",
        slave_addr,
        {"event_path": event_path, "result_str": result_str, "name": name},
    )
    return json.loads(result)
