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
    func_body = "def cronjob_function(**kwargs):\n"

    match input["action"]:
        case 'do_this_later':
            func_body = func_body + f"    do_this_later({input["params"]})"
        case 'notify_user':
            raise NotImplementedError("Not implemented user notifications yet")
        case _:
            raise ValueError("INVALID ACTION")

    if input['cronjob_type'] == "once":
        conn.execute("""
    INSERT INTO cronjob_once(body, start_after) VALUES(%s, %s);
                     """, (func_body, time.time() + input['run_after_or_every_s']))
    elif input['cronjob_type'] == "loop":
        conn.execute("""
    INSERT INTO cronjob_loop(body, execute_every) VALUES(%s, %s);
                     """, (func_body, input['run_after_or_every_s']))








def do_this_later(ai_instruction: str):
    conn = conn_factory()
    conn.execute("""
SELECT new_slave(NULL, %s);
                 """, (f"Perform the following actions: '{ai_instruction}'",))
