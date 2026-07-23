#!/usr/bin/env python3

import asyncio
from functools import partial
import re
from typing import Callable, Coroutine

from nats.aio.client import Client
from psycopg.rows import TupleRow

from ..rmt.main import activate_as_master
from ..utils.conn_factory import Conn, conn_factory
from ..utils.logger import log_json
from .types import ConsumerCallRmt, ConsumerData, ConsumerExecuteSlave, Event, EventConsumer, connect_nats

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

    for consumer_raw in event_consumers_fetch:

        consumer = build_consumer_data(consumer_raw)

        result.append(
            create_consumer(consumer, nt)
        )

    return result


def build_consumer_data(row: TupleRow) -> ConsumerData:
    """
    This function was built for the load_event_consumers and the exact querry used there.
    Dont reuse this for the love of god.
    """
    match row[1]:
        case "call_rmt":
            return ConsumerCallRmt(
                *[r for r in row] # NOTE : Make sure the order actually matches!
            )
        case 'execute_slave':
            return ConsumerExecuteSlave(
                *[r for r in row]
            )
        case _:
            raise ValueError(f"Unknown action type {row[1]}.")


async def consumer_outer(consumer_in: Callable[[Event, ConsumerData], None],
                         consumer_data: ConsumerData,
                         nt: Client) -> None:
    sub = await nt.subscribe(consumer_data.event_path)
    async for event in sub.messages:
        event = Event(event.subject, event.data.decode())
        consumer_in(event, consumer_data)
        log_json({
            'type': 'event',
            'subtype': 'consumer',
            'event_path': consumer_data.action_type
        })

def call_rmt(event: Event, consumer_data: ConsumerCallRmt) -> None: # TODO : Refactor these into async.
    conn = conn_factory()
    consumer_data.args['data'] = event.payload
    consumer_data.args['subject'] = event.event_path
    with conn.transaction():
        activate_as_master(consumer_data.rmt_id, conn, inputs=consumer_data.args)

def execute_slave(event: Event, consumer_data: ConsumerExecuteSlave) -> None:
    conn = conn_factory()

    instruction = consumer_data.instruction
    instruction = re.sub(re.escape('${{data}}'), event.payload, instruction, flags=re.DOTALL)
    instruction = re.sub(re.escape('${{subject}}'), event.event_path, instruction, flags=re.DOTALL)

    with conn.transaction():
        conn.execute("""
    PERFORM new_slave(NULL, %s, p_slave_scope := %s);
                     """, (instruction, consumer_data.scope))

def create_consumer(consumer_data: ConsumerData, nt: Client) -> Coroutine[None, None, None]:
    match type(consumer_data):
        case ConsumerExecuteSlave():
            consumer_in = execute_slave
        case ConsumerCallRmt():
            consumer_in = call_rmt
        case _:
            raise ValueError(f"Action type unknown. Action type {consumer_data.action_type} is not found.")
    return partial(consumer_outer, consumer_in, consumer_data, nt)()

