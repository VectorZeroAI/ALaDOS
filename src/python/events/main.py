#!/usr/bin/env python3
"""
The main file of the event suite.
"""
import asyncio
import threading
from .event_gens_registry import EVENT_PRODUCERS

def event_recieving_thread() -> None:
    loop = asyncio.new_event_loop()
    for i in EVENT_PRODUCERS:
        loop.create_task(i)
    loop.run_forever()


def event_consumer_thread() -> None:
    loop = asyncio.new_event_loop()
    for i in 



def startup() -> None:
    threading.Thread(target=event_recieving_thread, daemon=False).start()
    print("Startup of the event based proactivity system finished.")
