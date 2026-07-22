#!/usr/bin/env python3
import threading
import time

import psycopg
from ...interrupts.main import InterruptInvokation

from ...utils.conn_factory import Conn, conn_factory
from .cronjob_registry import prepare_cronjob
from ...queue import global_interrupt_queue
from .types import SysState

def setup():
    threading.Thread(target=cronjob_executor, daemon=True).start()

def cronjob_executor():
    conn = conn_factory()
    
    cronjob_conn = conn_factory()
    notifies_conn = conn_factory()

    notifies_conn.execute("LISTEN cronjob_changes")

    cronjob_changed = threading.Event()

    def cronjob_changes_listener(conn: Conn):
        for n in conn.notifies():
            cronjob_changed.set()

    threading.Thread(target=cronjob_changes_listener, args=(notifies_conn,), daemon=True).start()

    sys_state = SysState(cronjob_conn)

    while True:

        cronjob_fetch = conn.execute("""
    SELECT addr, name, run_at, type, params FROM cronjobs_to_run LIMIT 1
                     """).fetchone() # cronjobs_to_run is a view, wich includes order ba ASC.

        if cronjob_fetch is None:
            cronjob_changed.wait()
            continue

        wait_time = cronjob_fetch[2] - time.time() # RUN AT IS EPOCH
        changed_flag = cronjob_changed.wait(wait_time if wait_time > 0 else 0.001)

        if changed_flag:
            cronjob_changed.clear()
            continue

        try:
            
            cronjob = prepare_cronjob(cronjob_fetch[1], sys_state=sys_state, args=cronjob_fetch[4])
            global_interrupt_queue.put(InterruptInvokation("execute_cronjob", {'cronjob': cronjob}))

        except Exception as e:
            match cronjob_fetch[3]:
                case "cronjob_once":
                    conn.execute("""
            UPDATE cronjob_once SET error = TRUE, error_text = %s WHERE addr = %s;
                                 """, (str(e), cronjob_fetch[0]))
                case "cronjob_loop":
                     conn.execute("""
            UPDATE cronjob_loop SET error = TRUE, error_text = %s WHERE addr = %s;
                                  """, (str(e), cronjob_fetch[0]))
                case _:
                     raise psycopg.DatabaseError(f"unexpected type. Type got {cronjob_fetch[3]}, expected cronjob_once OR cronjob_loop")
        else:
            match cronjob_fetch[3]:
                case "cronjob_once":
                    conn.execute("""
            UPDATE cronjob_once SET finished = TRUE WHERE addr = %s;
                                 """, (cronjob_fetch[0], ))
                case "cronjob_loop":
                     conn.execute("""
            UPDATE cronjob_loop SET last_ran = %s WHERE addr = %s;
                                  """, (time.time(), cronjob_fetch[0]))
                case _:
                     raise psycopg.DatabaseError(f"unexpected type. Type got {cronjob_fetch[3]}, expected cronjob_once OR cronjob_loop")
