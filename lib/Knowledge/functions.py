#!/usr/bin/env python3
from .._.main import call

async def read(id: int|str, slave_addr: int) -> str:
    """ Read the knowledge entry. """
    return await call("K_read", slave_addr, {"id": id})
