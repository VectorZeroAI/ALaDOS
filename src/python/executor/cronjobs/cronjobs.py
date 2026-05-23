#!/usr/bin/env python3

from ...utils.conn_factory import conn_factory
from .cronjob_registry import register_cronjob

@register_cronjob('ai_perform_action_later')
def do_this_later(ai_instruction: str):
    conn = conn_factory()
    conn.execute("""
SELECT new_slave(NULL, %s);
                 """, (f"Perform the following actions: '{ai_instruction}'",))
