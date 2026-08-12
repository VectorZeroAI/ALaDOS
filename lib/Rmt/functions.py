#!/usr/bin/env python3
#!/usr/bin/env python3
"""
RMT (Reusable Master Template) client library.

Provides async wrappers for calling RMT-related syscalls from a slave context.
Mimics the pattern of knowledge functions (k_*).
"""

import json
from typing import Sequence

from .._.main import call
from ALaDOS.src.python.utils.sr_edit import SearchAndReplaceBlock # pyright: ignore


async def create_from_range(
    slave_addr: int,
    start_id: int | str,
    end_id: int | str,
    description: str,
    name: str | None = None,
) -> int:
    """
    Creates a reusable master template from a range of items (slaves).
    Traverses the live execution history to find the slaves between start and end,
    inclusively, and builds an RMT.
    Does not include variables; likely needs further editing.
    """
    result = await call(
        "rmt_create_from_range",
        slave_addr,
        {
            "start_id": start_id,
            "end_id": end_id,
            "description": description,
            "name": name,
        },
    )
    return int(result)


async def serialize(slave_addr: int, rmt_id: int | str) -> dict[str, str]:
    """
    Serialises an RMT into a readable format.
    Returns a dict with keys "dsl" and "description".
    """
    result = await call("rmt_serialize", slave_addr, {"id": rmt_id})
    return json.loads(result)


async def create_from_dsl(
    slave_addr: int,
    dsl: str,
    description: str,
    name: str | None = None,
) -> int:
    """
    Creates an RMT from a Domain Specific Language (DSL) string.
    Returns the new RMT's address.
    """
    result = await call(
        "rmt_create_from_dsl",
        slave_addr,
        {"dsl": dsl, "description": description, "name": name},
    )
    return int(result)


async def create_from_master(
    slave_addr: int,
    master_id: int | str,
    description: str,
    name: str | None = None,
) -> int:
    """
    Creates an RMT from an existing master execution.
    Returns the new RMT's address.
    """
    result = await call(
        "rmt_create_from_master",
        slave_addr,
        {"master_id": master_id, "description": description, "name": name},
    )
    return int(result)


async def edit_description(
    slave_addr: int,
    rmt_id: int | str,
    new_description: str,
) -> None:
    """
    Updates the description of an RMT.
    Returns nothing.
    """
    await call(
        "rmt_edit_description",
        slave_addr,
        {"rmt_id": rmt_id, "new_description": new_description},
    )


async def delete_node(
    slave_addr: int,
    rmt_slave_id: int | str,
    template_id: int | str,
    concatenate: bool = True,
) -> None:
    """
    Deletes a node from an RMT.
    If concatenate is True, reconnects the DAG (1->2->3 becomes 1->3).
    Otherwise, leaves a gap.
    Returns nothing.
    """
    await call(
        "rmt_slave_edit_delete_node",
        slave_addr,
        {
            "rmt_slave_id": rmt_slave_id,
            "template_id": template_id,
            "concatenate": concatenate,
        },
    )


async def insert_node(
    slave_addr: int,
    rmt_id: int | str,
    instruction: str,
    name: str | None = None,
    scope: str = "general",
    depends_on: Sequence[int | str] = [],
    required_by: Sequence[int | str] = [],
) -> dict[str, int]:
    """
    Inserts a new node into an RMT with given relationships.
    Returns a dict with keys "rmt_addr" and "node_addr".
    """
    result = await call(
        "rmt_slave_edit_insert_node",
        slave_addr,
        {
            "rmt_id": rmt_id,
            "instruction": instruction,
            "name": name,
            "scope": scope,
            "depends_on": list(depends_on),
            "required_by": list(required_by),
        },
    )
    return json.loads(result)


async def activate_as_master(
    slave_addr: int,
    rmt_id: int | str,
    inputs: dict[str, str],
    depends_on: Sequence[int | str] = [],
    required_by: Sequence[int | str] = [],
) -> int:
    """
    Activates an RMT as a master, providing variable substitutions.
    Returns the new master's address.
    """
    result = await call(
        "rmt_activate_as_master",
        slave_addr,
        {
            "rmt_id": rmt_id,
            "inputs": inputs,
            "depends_on": list(depends_on),
            "required_by": list(required_by),
        },
    )
    return int(result)


async def edit_node_instruction(
    slave_addr: int,
    node_id: int | str,
    sr_block: SearchAndReplaceBlock,
) -> None:
    """
    Edits the instruction of an RMT slave node using a search‑replace block.
    Returns nothing.
    """
    await call(
        "rmt_slave_edit_instruction",
        slave_addr,
        {"node_id": node_id, "sr_block": sr_block},
    )


async def change_node_scope(
    slave_addr: int,
    node_id: int | str,
    new_scope: str,
) -> None:
    """
    Changes the scope of an RMT slave node.
    Returns nothing.
    """
    await call(
        "rmt_slave_edit_scope",
        slave_addr,
        {"node_id": node_id, "new_scope": new_scope},
    )
