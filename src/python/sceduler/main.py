#!/usr/bin/env python3

"""
The sceduler subsystem.

Has the function setup(), wich first reconstructs the state in memory from the DB, and then spawns the thread. 

Consists of 1 thread, that listens on the "new_result_inserted" postgres chanell,
Tracks wich tasks are already being executed, and executes all the other tasks. 
"""

from pydantic import TypeAdapter
from ..utils.conn_factory import conn_factory
from ..executor.queue import executor_queue
from ..executor.types import instr_json
from .goal_stack.context import resolve_context
import psycopg
import threading

instr_json_validator = TypeAdapter(instr_json)

def slave_addr_to_instr(slave_addr: int, conn: psycopg.Connection) -> instr_json:
    """ resolves a slave addr to an instruction object, including context resolution. """
    context_prefetch = conn.execute("""
    SELECT instruction, master_addr, result_name, result_addr FROM slaves WHERE addr = %s;
                                    """, (slave_addr,)).fetchone()

    assert context_prefetch is not None

    context = resolve_context({
        "addr": slave_addr,
        "instruction": context_prefetch[0],
        "master_addr": context_prefetch[1],
        "result_name": context_prefetch[2]
        })

    instruction = instr_json_validator.validate_python({
        "result_addr": context_prefetch[3],
        "instruction": context_prefetch[0],
        "master_addr": context_prefetch[1],
        "context": context
        })
    return instruction

def new_slave_listener_thread():
    """ The sceduler thread that listens to Postgres telling it what slaves are unblocked, and sceduling them for execution."""
    try:
        conn = conn_factory()
        qconn = conn_factory()
        conn.execute("LISTEN slaves_ready")
        print("sceduler_listener_thread_ready")
        for n in conn.notifies():
            try:
                if n.channel != "slaves_ready":
                    continue
                instr = slave_addr_to_instr(int(n.payload), qconn)
                executor_queue.put(instr)
            except Exception as e:
                print(f"sceduler new_slave_listener_thread errored: {e}")
                # TODO : IMPLEMENT ACTUAL LOGGING
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            qconn.close()
        except Exception:
            pass
               

def setup():
    conn = conn_factory()

    unblocked_slave_addrs = conn.execute("""
    SELECT s.addr FROM slaves s
    WHERE NOT EXISTS (
        SELECT 1 FROM slave_req sr
        JOIN results r ON sr.req_addr = r.addr
        WHERE sr.slave_addr = s.addr
        AND r.ready IS FALSE
    )
                 """).fetchall()

    for addr in unblocked_slave_addrs:
        instruction = slave_addr_to_instr(addr[0], conn)
        executor_queue.put(instruction)

    threading.Thread(target=new_slave_listener_thread, daemon=True).start()
    print("startup of the sceduler finished.")
