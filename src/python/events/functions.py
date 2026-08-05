#!/usr/bin/env python3
"""
This is the file where the functions that use the events should be placed. 
"""

from ..utils.conn_factory import Conn
from ..types import ReferenceTo


def create_result_via_event(event_path: str, result_str: str, conn: Conn) -> ReferenceTo:
    """
    Creates a result that will be filled out with the event and a consumer to fill that event in.
    Does not handle wiring that result into the DAG, only handles the creation of the result itself. 
    Returns result addr.
    """

    event_consumers_addr = conn.execute_fetchval("""
    INSERT INTO event_consumers(event_path, action_type) VALUES(%s, 'fill_result') RETURNING addr
                 """, (event_path,))
    result_addr = conn.execute_fetchval("""
    INSERT INTO results DEFAULT VALUES RETURNING addr;
                                        """)

    # NOTE: Optimise into a single SQL querry with BEGIN END and DECLARE for speed, maybe.

    conn.execute("""
    INSERT INTO event_call_fill_result(addr, result_addr, result_str) VALUES(%s, %s, %s)
                 """, (event_consumers_addr, result_addr, result_str))

    return event_consumers_addr
