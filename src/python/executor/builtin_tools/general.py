#!/usr/bin/env python3
"""
This is the place where general tools that every profile should have live.
"""

from typing import get_args
from ..types import SlaveScope
from ..execute_tool import register_tool

ALL = get_args(SlaveScope)


@register_tool("K.create", ['all', 'general', 'context'])
def k_create(content: str, description: str, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ 
    Creates a knowledge item.
    The description is a short definition of the items contents for semantic similarity search.
    Content is the actual content, and name is name which can be used a access the item.
    Name of a knowledge item CANNOT be used in goal.add_slave required_results_names.
    """
    conn = _meta['conn']

    addr = conn.execute("SELECT new_addr();").fetchone()[0] # pyright: ignore
    conn.execute("""
    INSERT INTO knowledge (addr, content, description) VALUES (%s, %s, %s);
                 """, (addr, content, description))
    if name is not None:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr,name))

    _meta['_embedder_queue'].put(addr)

    return f"knowledge entry {name if name is not None else "No name"}@{addr} was created."

