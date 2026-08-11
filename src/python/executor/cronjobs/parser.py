#!/usr/bin/env python3
import time

from psycopg.types.json import Jsonb
from ...types import ReferenceTo

from ...utils.conn_factory import Conn
from .types import Cronjob


def insert_cronjob(input_cronjob: Cronjob, conn: Conn) -> ReferenceTo:

    if input_cronjob.cronjob_type == "once":
        addr = conn.execute_fetchval("""
        INSERT INTO cronjob_once(name, start_after, args) VALUES(%s, %s, %s) RETURNING addr;
        """, (
             input_cronjob.action, time.time() + input_cronjob.time, Jsonb(input_cronjob.params)
             )
        )
    else:
        addr = conn.execute_fetchval("""
        INSERT INTO cronjob_loop(name, execute_every, args) VALUES(%s, %s, %s) RETURNING addr;
        """, (
             input_cronjob.action, input_cronjob.time, Jsonb(input_cronjob.params)
             )
        )

    return addr

