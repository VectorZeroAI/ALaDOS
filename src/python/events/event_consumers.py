#!/usr/bin/env python3
"""

Event consumers, creation and loading. 

Guide on how to add a new type of event consumer:
    1: Add it to the DB. (HAS TO USE 2 collumns)
    2: Add it to the Querry in Load.
    3: Add the new dataclass
    4: Add the new case to the build consumers function
    5: Add the new inner consumer function
    6: Add the new case to the create_consumer

"""

import asyncio

from nats.aio.client import Client
from psycopg.rows import TupleRow

from ..rmt.main import activate_as_master
from ..utils.conn_factory import Conn, conn_factory
from ..utils.logger import log_json
from ..utils.connect_nats import connect_nats
from .types import (
    ConsumerCallRmt,
    ConsumerData,
    ConsumerExecuteSlave,
    ConsumerFillResult,
    Event,
    EventConsumer
)


def load_event_consumers(conn: Conn, loop: asyncio.AbstractEventLoop) -> list[EventConsumer]:
    """
    This function loads all the Event consumers from the DB for the consumer thread.

    Rules on what the subject / payload of events is and how its supposed to be sent out:
        subject is the event identifier / type, explaining the category of the event. 
        The payload itself will be the information on the exact event. 

    ${{data}} will be replaced with the payload at activation time, while 
    ${{subject}} will be replaced with the full event path at activation path.

    The actual consumer data object loading uses the build_consumer_data function,
        !!! thus argument order in event_consumers_fetch MUST match the expected one by the function !!!
    """

    event_consumers_fetch = conn.execute("""
    SELECT ec.event_path,
           ec.action_type,
           COALESCE(evr.rmt_addr::TEXT, evs.instruction, evfr.result_addr::TEXT),
           COALESCE(evr.args::TEXT, evs.scope::TEXT, evfr.result_str)
    FROM event_consumers ec
        LEFT JOIN event_call_rmt evr ON ec.addr = evr.addr
        LEFT JOIN event_call_execute_slave evs ON ec.addr = evs.addr
        LEFT JOIN event_call_fill_result evfr ON ec.addr = evfr.addr
                 """).fetchall()
    ## NOTE : NEVER EVER CHANGE ORDER WITHOUT CHANGING THE build_consumer_data FUNCTION!!!
    
    result: list[EventConsumer] = []

    nt = loop.run_until_complete(connect_nats())

    for consumer_raw in event_consumers_fetch:

        consumer = build_consumer_data(consumer_raw)

        result.append(
            consumer_outer(consumer, nt)
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
                row[0],
                row[1],
                int(row[2]),
                row[3]
            )
        case 'execute_slave':
            return ConsumerExecuteSlave(
                row[0],
                row[1],
                row[2],
                row[3]
            )
        case "fill_result":
            return ConsumerFillResult(
                row[0],
                row[1],
                int(row[2]),
                row[3]
            )
        case _:
            raise ValueError(f"Unknown action type {row[1]}.")


async def consumer_outer(consumer_data: ConsumerData,
                         nt: Client) -> None:
    sub = await nt.subscribe(consumer_data.event_path)
    loop = asyncio.get_running_loop()


    async for event in sub.messages:
        event = Event(event.subject, event.data.decode(), nt)

        match consumer_data:
            case ConsumerExecuteSlave():
                await loop.run_in_executor(None, execute_slave, event, consumer_data)
            case ConsumerCallRmt():
                await loop.run_in_executor(None, call_rmt, event, consumer_data)
            case ConsumerFillResult():
                await loop.run_in_executor(None, fill_result, event, consumer_data)
            case _:
                raise ValueError(f"Action type unknown. Action type {consumer_data.action_type} is not found.")

        log_json({
            'type': 'event',
            'subtype': 'consumer',
            'event_path': consumer_data.action_type
        })



def call_rmt(event: Event, consumer_data: ConsumerCallRmt) -> None:
    conn = conn_factory()
    consumer_data.args['data'] = event.payload
    consumer_data.args['subject'] = event.event_path
    with conn.transaction():
        activate_as_master(consumer_data.rmt_id, conn, inputs=consumer_data.args)
    conn.close()



def execute_slave(event: Event, consumer_data: ConsumerExecuteSlave) -> None:
    conn = conn_factory()

    instruction = consumer_data.instruction
    instruction = instruction.replace('${{data}}', event.payload)
    instruction = instruction.replace('${{subject}}', event.event_path)

    with conn.transaction():
        conn.execute("""
        SELECT new_slave(NULL, %s, p_slave_scope := %s);
                     """, (instruction, consumer_data.scope))
    conn.close() # TODO: Read psycopg docs on how to close connections correctly.



def fill_result(event: Event, consumer_data: ConsumerFillResult) -> None:
    conn = conn_factory()

    result_str = consumer_data.result_str 
    result_str = result_str.replace("${{data}}", event.payload)
    result_str = result_str.replace("${{event}}", event.event_path)

    with conn.transaction():
        conn.execute("""
        SELECT new_result(%s, %s)
                     """, (result_str, consumer_data.result_addr))

    conn.close()

