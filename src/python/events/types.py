#!/usr/bin/env python3

"""
The file where all the types are.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Coroutine, TypeAlias
import nats
from nats.aio.client import Client

EventConsumer: TypeAlias = Coroutine[None, None, None]

async def connect_nats() -> Client:
    return await nats.connect()

@dataclass(slots=True)
class Event:
    event_path: str
    payload: str
    __client: Client = field()
    __loop: asyncio.AbstractEventLoop = field()
    
    def __init__(self) -> None:
        self.__loop = asyncio.get_event_loop()
        self.__client = self.__loop.run_until_complete(connect_nats())

    def send(self) -> None:
        self.__loop.run_until_complete(
            self.__client.publish(
                self.event_path,
                self.payload.encode()
            )
        )

    async def send_async(self) -> None:
        await self.__client.publish(self.event_path, self.payload.encode())
