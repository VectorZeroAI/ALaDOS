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

class Event:
    """
    The Event class, with the send method.
    """
    event_path: str
    payload: str

    async def __init__(self,
                       event_path: str,
                       payload: str) -> None:
        self.event_path = event_path
        self.payload = payload
        self.__client = await connect_nats() ## TODO: Refactor so that there arent that many connections.

    async def send(self) -> None:
        await self.__client.publish(
            self.event_path,
            self.payload.encode()
        )


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

@dataclass(slots=True)
class ConsumerFillResult:
    event_path: str
    action_type: Literal['fill_result']
    result_addr: int
    result_str: str

ConsumerData: TypeAlias = Union[ConsumerCallRmt, ConsumerExecuteSlave, ConsumerFillResult]
"""
The consumer data needs to be updated with each new Consumer* dataclass.
Each Consumer* has to have event_path as str and action_type as its action_type literal. 
    TODO: reason if it actually does need that, and if false, remove it, since its kinda uselles actually,
    cause type is known from datatype directly, so its redundant information.
"""

@dataclass(slots=True)
class EventsConfig:
    filesystem_watch_dirs: list[PathLike]
