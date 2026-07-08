#!/usr/bin/env python3

import time
from typing import Literal, TypedDict, Any
import psycopg
from psycopg.types.json import Jsonb

from ...utils.conn_factory import conn_factory

CronjobActions = Literal['do_this_later', 'notify_user']

class CronjobExpression(TypedDict):
    """ The cronjob expression DSL """
    action: CronjobActions
    params: dict[str, Any]
    cronjob_type: Literal["loop","once"]
    run_after_or_every_s: int

def parse(input_cronjob: CronjobExpression, conn: psycopg.Connection = conn_factory()):

    if input_cronjob['cronjob_type'] == "once":
        conn.execute("""
    INSERT INTO cronjob_once(body, start_after, args) VALUES(%s, %s, %s);
                     """, (input_cronjob['action'], time.time() + input_cronjob['run_after_or_every_s'], Jsonb(input_cronjob['params']) ))
    elif input_cronjob['cronjob_type'] == "loop":
        conn.execute("""
    INSERT INTO cronjob_loop(body, execute_every, args) VALUES(%s, %s, %s);
                     """, (input_cronjob['action'], input_cronjob['run_after_or_every_s'], Jsonb(input_cronjob['params'])))

