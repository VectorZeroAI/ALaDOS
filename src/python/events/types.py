#!/usr/bin/env python3

"""
The file where all the types are.
"""

import asyncio
from dataclasses import dataclass, field
from functools import partial
from os import PathLike
from typing import Coroutine, Literal, TypeAlias, Union
import nats
from nats.aio.client import Client
from ..types import ReferenceTo
from ..executor.types import SlaveScope

EventConsumer: TypeAlias = Coroutine[None, None, None]

async def connect_nats() -> Client:
    return await nats.connect()

@dataclass(slots=True)
class Event:
    event_path: str
    payload: str
    __client: Client = field()
    __loop: asyncio.AbstractEventLoop = field()
    
    def __init__(self, event_path: str, payload: str) -> None:
        self.event_path = event_path
        self.payload = payload
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

@dataclass(slots=True)
class ConsumerCallRmt:
    event_path: str
    action_type: Literal['call_rmt']
    rmt_id: ReferenceTo
    args: dict[str, str]

@dataclass(slots=True)
class ConsumerExecuteSlave:
    event_path: str
    action_type: Literal['execute_slave']
    instruction: str
    scope: SlaveScope

ConsumerData: TypeAlias = Union[ConsumerCallRmt, ConsumerExecuteSlave]

@dataclass(slots=True)
class EventsConfig:
    filesystem_watch_dirs: list[PathLike]
