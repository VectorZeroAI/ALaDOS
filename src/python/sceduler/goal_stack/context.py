#!/usr/bin/env python3

from .types import SlaveObj
from ...utils.conn_factory import conn_factory

def resolve_context(slave_obj: SlaveObj):
    conn = conn_factory()
    curr = conn.cursor()

    curr.execute("""
    
                 """)
