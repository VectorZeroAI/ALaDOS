#!/usr/bin/env python3
"""
The main file of the event suite.

Handles the activation of the 2 threads, the event recievers thread and the event senders thread.
Uses NATS to route the events from senders to recievers.
Future: Maybe refactor from using NATS to using some sort of handrolled structure?
Like a list of partially applied policy functions?
But just maybe, and definetly later.
"""
import asyncio
import threading

from python.events.event_consumers import load_event_consumers
from python.utils.conn_factory import conn_factory
from .event_gens_registry import EVENT_PRODUCERS

def event_producer_thread() -> None:
    """
    The event producer thread with its own asyncio loop of event generators.
    IDK if its right or wrong to use the asyncio loop per thread pattern, but who the fuck cares.
    """
    loop = asyncio.new_event_loop()
    for i in EVENT_PRODUCERS:
        loop.create_task(i)
    loop.run_forever()


def event_consumer_thread() -> None:
    """
    The event consumer thread with its own asyncio loop.
    IDK if its right or wrong to use the asyncio loop per thread pattern, but who the fuck cares.
    """
    loop = asyncio.new_event_loop()
    conn = conn_factory()
    asyncio.gather(*load_event_consumers(conn, loop))
    loop.run_forever()

def startup() -> None:
    threading.Thread(target=event_producer_thread, daemon=True).start()
    threading.Thread(target=event_consumer_thread, daemon=True).start()
    print("Startup of the event based proactivity system finished.")
