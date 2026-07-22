#!/usr/bin/env python3

import asyncio
import re

from ..rmt.main import activate_as_master
from ..utils.conn_factory import Conn, conn_factory
from ..utils.logger import log_json
from .types import EventConsumer, connect_nats

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
        match consumer[1]:
            case 'call_rmt':
                async def the_event_consumer() -> None:
                    sub = await nt.subscribe(consumer[0])
                    async for event in sub.messages:
                        conn = conn_factory()
                        consumer[4]['data'] = event.data.decode()
                        consumer[4]['subject'] = event.subject
                        with conn.transaction():
                            activate_as_master(consumer[2], conn, inputs=consumer[4])
                        log_json({
                            'type': 'proactivity',
                            'subtype': 'consumer',
                            'event_path': event.subject,
                            'data': event.data.decode(),
                            'status': 'normal'
                        })

                result.append(the_event_consumer())

            case 'execute_slave':
                async def the_event_consumer() -> None:
                    sub = await nt.subscribe(consumer[0])
                    async for event in sub.messages:
                        conn = conn_factory()
                        
                        instruction = consumer[3]
                        instruction = re.sub(re.escape('${{data}}'), event.data.decode(), instruction, flags=re.DOTALL)
                        instruction = re.sub(re.escape('${{subject}}'), event.subject, instruction, flags=re.DOTALL)

                        with conn.transaction():
                            conn.execute("""
                    PERFORM new_slave(NULL, %s, p_slave_scope := %s);
                                         """, (instruction, consumer[4]))
                        
                        log_json({
                            'type': 'proactivity',
                            'subtype': 'consumer',
                            'event_path': event.subject,
                            'data': event.data.decode(),
                            'status': 'normal'
                        })

                result.append(the_event_consumer())
