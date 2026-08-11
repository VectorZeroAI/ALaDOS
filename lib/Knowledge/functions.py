#!/usr/bin/env python3
from .._.main import call

async def create(slave_addr: int, content: str, description: str, name: str|None = None) -> int:
    """ Create a knowledge entry. """
    result = await call("k_create", slave_addr, {"content": content, "description": description, "name": name})
    return int(result)

async def read(id: int|str, slave_addr: int) -> str:
    """ Read the knowledge entry. """
    return await call("k_read", slave_addr, {"id": id})

async def edit(id: int|str, slave_addr: int, content_change: str|None, description_change: str|None) -> None:
    """ Edits the knowledge entry. Returns nothing. """
    # TODO : Consider how the raising strategy should work, for for example, occ fail.

    await call("k_edit", slave_addr, {"content_change": content_change, "description_change": description_change})
