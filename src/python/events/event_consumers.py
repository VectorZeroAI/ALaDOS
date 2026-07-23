#!/usr/bin/env python3

import asyncio
from functools import partial
import re
from typing import Callable, Coroutine

from nats.aio.client import Client
from psycopg.rows import TupleRow
from ..types import ReferenceTo

from ..rmt.main import activate_as_master
from ..utils.conn_factory import Conn, conn_factory
from ..utils.logger import log_json
from .types import Event, EventConsumer, connect_nats

def load_event_consumers(conn: Conn, loop: asyncio.AbstractEventLoop) -> list[EventConsumer]:
    """
    This function loads all the Event consumers from the DB for the consumer thread.

    Rules on what the subject / payload of events is and how its supposed to be sent out:
        subject is the event identifier / type, explaining the category of the event. 
        The payload itself will be the information on the exact event. 

    ${{data}} will be replaced with the payload at activation time, while 
    ${{subject}} will be replaced with the full event path at activation path.
    """

    event_consumers_fetch = conn.execute("""
    SELECT ec.event_path,
           ec.action_type,
           COALESCE(evr.rmt_addr, evc.instruction),
           COALESCE(evr.args, evs.scope)
    FROM event_consumers ec
        LEFT JOIN event_call_rmt evr ON ec.addr = evr.addr
        LEFT JOIN event_call_execute_slave evs ON ec.addr = evs.addr
                 """).fetchall()
    
    result: list[EventConsumer] = []

    nt = loop.run_until_complete(connect_nats())

    for consumer in event_consumers_fetch:
        result.append(
            create_consumer(consumer, nt)
        )

    return result


async def consumer_outer(consumer_in: Callable[[Event, TupleRow], None],
                         consumer_data: TupleRow,
                         nt: Client) -> None:
    sub = await nt.subscribe(consumer_data[0])
    async for event in sub.messages:
        event = Event(event.subject, event.data.decode())
        consumer_in(event, consumer_data)
        log_json({})

def call_rmt(event: Event, consumer_data: TupleRow) -> None:
    conn = conn_factory()
    consumer_data[4]['data'] = event.payload
    consumer_data[4]['subject'] = event.event_path
    with conn.transaction():
        activate_as_master(consumer_data[3], conn, inputs=consumer_data[4])

def execute_slave(event: Event, consumer_data: TupleRow) -> None:
    conn = conn_factory()

    instruction = consumer_data[3]
    instruction = re.sub(re.escape('${{data}}'), event.payload, instruction, flags=re.DOTALL)
    instruction = re.sub(re.escape('${{subject}}'), event.event_path, instruction, flags=re.DOTALL)

    with conn.transaction():
        conn.execute("""
    PERFORM new_slave(NULL, %s, p_slave_scope := %s);
                     """, (instruction, consumer_data[4]))

def create_consumer(consumer_data: TupleRow, nt: Client) -> Coroutine[None, None, None]:
    match consumer_data[0]:
        case 'execute_slave':
            consumer_in = execute_slave
        case 'call_rmt':
            consumer_in = call_rmt
        case _:
            raise ValueError(f"Action type unknown. Action type {consumer_data[0]} is not found.")
    return partial(consumer_outer, consumer_in, consumer_data, nt)()

