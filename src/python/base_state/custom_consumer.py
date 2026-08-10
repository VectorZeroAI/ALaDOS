#!/usr/bin/env python3
"""
File for the implementation of the cutom consumer.

Primarily for syscalls and lib right now, but later on may be reused for a lot of things.
"""

from typing import Callable

from python.events.types import Event, connect_nats


async def consumer_outer(consumer_inner: Callable[[Event, str], None], event_path: str):
    nt = await connect_nats()
    sub = await nt.subscribe(event_path)

    async for event_raw in sub.messages:
        event_obj = Event(event_raw.subject, event_raw.data.decode(), nt)
        consumer_inner(event_obj, event_path)
