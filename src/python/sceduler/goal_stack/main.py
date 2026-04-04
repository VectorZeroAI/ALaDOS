#!/usr/bin/env python3
from .types import SlaveAddr, SlaveObj
from ...utils.conn_factory import conn_factory
from pydantic import TypeAdapter

slave_pydantic_obj = TypeAdapter(SlaveObj)

def submit_slave(slave_addr: SlaveAddr):
    conn = conn_factory()
    curr = conn.cursor()
    
    slave_fetch = curr.execute("""
    SELECT master_addr, result_addr, instruction, result_name FROM slaves WHERE addr = ?
                 """, (slave_addr,)).fetchall()
    
    slave_obj_raw = {
            "addr": slave_addr,
            "master_addr": slave_fetch[0],
            "result_addr": slave_fetch[1],
            "instruction": slave_fetch[2],
            "result_name": slave_fetch[3]
            }
    slave_obj = slave_pydantic_obj.validate_python(slave_obj_raw)
    
    context = 

