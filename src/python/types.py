#!/usr/bin/env python3

from dataclasses import dataclass, field
from typing import Literal, Protocol, Self, TypeAlias, Union

from nats.aio.msg import Msg
from pydantic import JsonValue

from .utils.uqueue import Uqueue

ValidTables: TypeAlias = Union[Literal['executables'],
                               Literal['knowledge'],
                               Literal['addrs'],
                               Literal['results'],
                               Literal['slaves'],
                               Literal['masters'],
                               Literal['slave_req'],
                               Literal['names'],
                               Literal['logs'],
                               Literal['master_context'],
                               Literal['master_load'],
                               ]

ReferenceTo: TypeAlias = int


@dataclass(slots=True)
class ToolCall:
    """ A single tool call, directly executable """
    tool: str
    args: dict[str, JsonValue] = field(default_factory=dict[str, JsonValue])

SyscallsQueue: TypeAlias = Uqueue[tuple[ToolCall, Msg]]
SyscallsQueues: TypeAlias = dict[int, SyscallsQueue]


class SingletonMeta(type):
    _instances: dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if SingletonMeta._instances.get(cls) is None:
            SingletonMeta._instances[cls] = super().__call__(*args, **kwargs)
        return SingletonMeta._instances[cls]

class Singleton(metaclass=SingletonMeta):
    pass

class CacheManager[T_i, T_o](Protocol):
    """
    Parent class for all the Cache managers, as well as protocoll for all the Cache Managers.
    
    Additional notes to the Protocol: 
        The Cache format is LRU, with limit being the limit.
        Invalidate function must be called to invalidate.
        It must be thread-safe.
        
        If item is not found in cache, it must resolve it and still give it.
        __getitem__ can not raise, and can only return the item,
        depending on it being cached or not, it takes a long or a short time.


    !!! Never make this into an inheritable class because you still have to manually enter the invalidation TRIGGER and Listener and Interrupt !!!
    """

    def __init__(self, limit: int) -> None:
        ...

    def invalidate(self, item: T_i) -> None:
        ...

    def __getitem__(self, item: T_i) -> T_o:
        ...
