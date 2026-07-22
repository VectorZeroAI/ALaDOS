#!/usr/bin/env python3
import time

from psycopg import Connection
from psycopg.types.json import Jsonb

from ...utils.conn_factory import conn_factory_raw
from .types import Cronjob


def insert_cronjob(input_cronjob: Cronjob, conn: Connection = conn_factory_raw()):

    if input_cronjob.cronjob_type == "once":
        conn.execute("""
    INSERT INTO cronjob_once(name, start_after, args) VALUES(%s, %s, %s);
         """, (input_cronjob.action, time.time() + input_cronjob.time, Jsonb(input_cronjob.params)))
    elif input_cronjob.cronjob_type == "loop":
        conn.execute("""
    INSERT INTO cronjob_loop(name, execute_every, args) VALUES(%s, %s, %s);
         """, (input_cronjob.action, input_cronjob.time, Jsonb(input_cronjob.params)))

