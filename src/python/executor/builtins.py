#!/usr/bin/env python3

from typing import Optional, overload
from .execute_tool import register_tool
from ..utils.conn_factory import conn_factory

# EXAMPLE: 
@register_tool("print.to_console")
def print_to_console(input_str: str) -> None:
    print(input_str)

@register_tool("K.write")
def k_write(content: str, description: str, name: str|None = None, _master_id: int) -> None:
    conn = conn_factory()

    addr = conn.execute("SELECT new_addr();").fetchone()[0] # pyright: ignore
    conn.execute("""
    INSERT INTO knowledge (addr, content, description) VALUES (%s, %s, %s);
                 """, (addr, content, description))
    if name is not None:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr,name))
    conn.close()
    return True

@register_tool("K.read")
def k_read(addr: int|None, name: str|None, _master_id: int) -> str:
    """ Reads a knowledge item by address or by name. One of those must be provided. """
    conn = conn_factory()
    if addr is not None:
        result = conn.execute("""
        SELECT content FROM knowledge WHERE addr = %s
                     """, (addr,)).fetchone()[0]
    elif name is not None:
        result = conn.execute("""
        SELECT k.content FROM knowledge JOIN names n ON k.addr = n.addr WHERE n.name = %s
                     """, (name,)).fetchone()[0]
    else:
        raise TypeError("ADDR OR NAME MUST BE PROVIDED")
    return result


