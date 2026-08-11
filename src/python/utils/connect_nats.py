#!/usr/bin/env python3
import nats
from nats.aio.client import Client

async def connect_nats() -> Client:
    return await nats.connect()
