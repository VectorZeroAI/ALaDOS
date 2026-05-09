#!/usr/bin/env python3
"""
This is the place where general tools that every profile should have live.
"""

from typing import get_args
from ..types import SlaveScope, _ExecToolMetaData, ActionConfirmation
from ..execute_tool import register_tool
from ...utils.search_and_replace_block_parser import sr_block_parser, search_and_replace_block

ALL = get_args(SlaveScope)

@register_tool("K.edit", ALL)
def k_edit(addr: int|None = None,
           name: str|None = None,
           description_change: search_and_replace_block = None,
           content_change: search_and_replace_block = None,
           _meta: _ExecToolMetaData = None
           ) -> ActionConfirmation:
    """
    Edits a knowledge entry. 
    Either addr or name must be provided
    change is in the same format as tool.edits change format.
    """
    conn = _meta['conn']
    if addr is None:
        addr = conn.execute("""
        SELECT resolve_name(%s);
                     """, (name,)).fetchone()[0]

    if content_change is not None:
        old_k = conn.execute("""
        SELECT content FROM knowledge WHERE addr = %s;
                             """, (addr,)).fetchone()[0]
        assert isinstance(old_k, str)
        search, replace = sr_block_parser(content_change)
        new_k = old_k.replace(search, replace)
        conn.execute("""
        UPDATE knowledge SET content = %s WHERE addr = %s;
                     """, (new_k, addr))

    if description_change is not None:
        old_d = conn.execute("""
        SELECT description FROM knowledge WHERE addr = %s;
                             """, (addr,)).fetchone()[0]
        assert isinstance(old_d, str)
        search, replace = sr_block_parser(description_change)
        new_d = old_d.replace(search, replace)
        conn.execute("""
        UPDATE knowledge SET description = %s WHERE addr = %s;
                     """, (new_d, addr))

        _meta['_embedder_queue'].put(addr)

    return f"Edited the knowledge item {name if name is not None else "Nameless"}@{addr}"

@register_tool("K.create", ALL)
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



@register_tool("K.read", ALL)
def k_read(addr: int|None = None, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ Reads a knowledge item by address or by name. One of those must be provided. """
    conn = _meta['conn']
    if addr is not None:
        result = conn.execute("""
        SELECT content FROM knowledge WHERE addr = %s
                     """, (addr,)).fetchone()[0]
    elif name is not None:
        result = conn.execute("""
        SELECT k.content FROM knowledge k JOIN names n ON k.addr = n.addr WHERE n.name = %s
                     """, (name,)).fetchone()[0]
    else:
        raise TypeError("ADDR OR NAME MUST BE PROVIDED")

    return f"Knowledge entry {name if name is not None else "no name"}@{addr} contents: {result}."



