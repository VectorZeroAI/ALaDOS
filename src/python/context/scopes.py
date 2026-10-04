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
    
    
