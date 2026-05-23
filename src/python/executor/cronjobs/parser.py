#!/usr/bin/env python3

import time
from typing import Literal, TypedDict, Any

from ...utils.conn_factory import conn_factory

CronjobActions = Literal['do_this_later', 'notify_user']

class CronjobExpression(TypedDict):
    """ The cronjob expression DSL """
    action: CronjobActions
    params: dict[str, Any]
    cronjob_type: Literal["loop","once"]
    run_after_or_every_s: int

def parse(input: CronjobExpression):
    conn = conn_factory()

    if input['cronjob_type'] == "once":
        conn.execute("""
    INSERT INTO cronjob_once(body, start_after) VALUES(%s, %s);
                     """, (input['action'], time.time() + input['run_after_or_every_s']))
    elif input['cronjob_type'] == "loop":
        conn.execute("""
    INSERT INTO cronjob_loop(body, execute_every) VALUES(%s, %s);
                     """, (input['action'], input['run_after_or_every_s']))








