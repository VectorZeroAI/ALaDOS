#!/usr/bin/env python3

from .conn_factory import conn_factory

def create_rmt_from_master(master_addr: int) -> int:
    conn = conn_factory()

    conn.execute("""
    SELECT 
                 """)


