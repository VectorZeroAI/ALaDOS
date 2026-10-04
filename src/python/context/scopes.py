#!/usr/bin/env python3
"""
The file containing the implementation of the python side of the new dynamic scopes.
"""

from ..utils.conn_factory import Conn

from ..types import Addr, Name

def load_scope(id: Name|Addr, conn: Conn) -> str:
    """
    Load a scope from Database
    """
    addr = conn.resolve_to_addr(id)
    fetch = conn.execute("""
    SELECT s_t.tool_addr, t_n.name, v.description, e.header
    FROM scopes_tools s_t
        LEFT JOIN names t_n ON t_n.addr = s_t.tool_addr
        JOIN executables e ON e.addr = s_t.tool_addr
        JOIN vector_ops v ON v.addr = e.addr
                 """, [addr,]).fetchall()
    
    return "\n\n------\n\n".join([f"{elem[1]}@{elem[0]}:\n{elem[2]}\n{elem[3]}" for elem in fetch])
