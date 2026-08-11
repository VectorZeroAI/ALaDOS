#!/usr/bin/env python3
"""
File for the implementation of the cutom consumer.

Primarily for syscalls and lib right now, but later on may be reused for a lot of things.
"""

from typing import Callable

from nats.aio.client import Client
from nats.aio.msg import Msg

from ..utils.connect_nats import connect_nats

async def consumer_outer(consumer_inner: Callable[[Msg, Client], None], event_path: str):
    nt = await connect_nats()
    sub = await nt.subscribe(event_path)

    async for event_raw in sub.messages:
        consumer_inner(event_raw, nt)
