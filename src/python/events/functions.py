#!/usr/bin/env python3
"""
This is the file where the functions that use the events should be placed. 
"""

from psycopg.types.json import Jsonb
from dataclasses import dataclass

from ..executor.types import SlaveScope
from ..types import ReferenceTo
from ..utils.conn_factory import Conn

@dataclass(slots=True)
class ResultViaEventReturn:
    result_addr: ReferenceTo
    consumer_addr: ReferenceTo

def create_result_via_event(event_path: str, result_str: str, conn: Conn) -> ResultViaEventReturn:
    """
    Creates a result that will be filled out with the event and a consumer to fill that event in.
    Does not handle wiring that result into the DAG, only handles the creation of the result itself. 
    Returns both result_addr and consumer_addr.
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

    return ResultViaEventReturn(result_addr, event_consumers_addr)


def register_reaction_rmt(event_path: str, rmt_addr: ReferenceTo, args: dict[str, str], conn: Conn) -> ReferenceTo:
    """
    Register the event reaction as an RMT.
    All of the patterns are explained in the consumer load function docs and in builtins.

    Returns consumer addr
    """
    consumer_addr = conn.execute_fetchval("""
    INSERT INTO event_consumers(event_path, action_type) VALUES(%s, 'call_rmt') RETURNING addr;
                 """, (event_path,))

    conn.execute("""
    INSERT INTO event_call_rmt(addr, rmt_addr, args) VALUES(%s, %s, %s)
                 """, (consumer_addr, rmt_addr, Jsonb(args)))
    
    return consumer_addr


def register_reaction_execute_slave(event_path: str, instruction: str, scope: SlaveScope, conn: Conn) -> ReferenceTo:
    """
    Register event reaction as slave.
    All of the patterns are explained in the consumer load function docs and in builtins.

    Returns consumer addr.
    """
    consumer_addr = conn.execute_fetchval("""
    INSERT INTO event_consumers(event_path, action_type) VALUES(%s, 'execute_slave') RETURNING addr;
                                          """, (event_path,))

    conn.execute("""
    INSERT INTO event_call_execute_slave(addr, instruction, scope) VALUES(%s, %s, %s);
                 """, (consumer_addr, instruction, scope))
    
    return consumer_addr
