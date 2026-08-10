#!/usr/bin/env python3
from .._.main import call

async def create(slave_addr: int, content: str, description: str, name: str|None = None) -> int:
    """ Create a knowledge entry. """
    result = await call("K_create", slave_addr, {"content": content, "description": description, "name": name})
    return int(result)

async def read(id: int|str, slave_addr: int) -> str:
    """ Read the knowledge entry. """
    return await call("K_read", slave_addr, {"id": id})

