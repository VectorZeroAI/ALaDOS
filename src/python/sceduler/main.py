#!/usr/bin/env python3

"""
The sceduler subsystem.

Has the function setup(), wich first reconstructs the state in memory from the DB, and then spawns the thread. 

Consists of 1 thread, that listens on the "new_result_inserted" postgres chanell,
Tracks wich tasks are already being executed, and executes all the other tasks. 
"""

from ..utils.conn_factory import conn_factory

def setup():
    conn = conn_factory()
    curr = conn.cursor()

    slaves_to_execute = curr.execute("""
SELECT s.addr FROM slaves s
WHERE NOT EXISTS (
    SELECT 1 FROM slave_req sr
    JOIN results r ON sr.req_addr = r.addr
    WHERE sr.slave_addr = s.addr
    AND r.ready IS FALSE
)
                 """).fetchall()
    for slave in slaves_to_execute:
        
