#!/usr/bin/env python3
from typing import TypedDict
import psycopg
from ..utils.conn_factory import conn_factory
import threading
import asyncio

class SysState(TypedDict):
    conn: psycopg.Connection


def setup():
    conn = conn_factory()
    
    threading.Thread(target=cronjob_executor, daemon=True).start()


async def cronjob_executor():
    conn = conn_factory()
    conn.execute("LISTEN 'cronjob_changes'")

    cronjob_changed = asyncio.Future()

    async def cronjob_changes_listener(conn: psycopg.Connection):
        while True:
            for n in conn.notifies():
                cronjob_changed.set_result(True)

    while True:

        sys_state: SysState = {
            "conn": conn_factory()
        }

        cronjob_fetch = conn.execute("""
SELECT addr, body, run_at, type FROM cronjobs_to_run LIMIT 1
                     """).fetchone()
        if cronjob_fetch is None:
            await asyncio.wait_for(cronjob_changed, None)
            continue

        exec(cronjob_fetch[1]) # Do you have a better solution? Propse a better solution if you want. 
        # I need to execute a cronjob stored in postgres. 

        try:
            await asyncio.wait_for(cronjob_changed, cronjob_fetch[2])
        except TimeoutError:
            pass
        else:
            continue

        try:
            cronjob_function(sys_state)
            # NOTE: IGNORE BECAUSE THIS FUNCTION WILL APPEAR AFTER EXEC
        except NameError:
            match cronjob_fetch[3]:
                case "cronjob_once":
                    conn.execute("""
            UPDATE cronjob_once SET error = TRUE WHERE addr = %s;
                                 """, (cronjob_fetch[0],))
                case "cronjob_loop":
                     conn.execute("""
            UPDATE cronjob_loop SET error = TRUE WHERE addr = %s;
                                  """, (cronjob_fetch[0],))
                case _:
                     raise psycopg.DataError(f"unexpected type. Type got {cronjob_fetch[3]}, expected cronjob_once OR cronjob_loop")
