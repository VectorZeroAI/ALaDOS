#!/usr/bin/env python3

from dataclasses import dataclass, field
from typing import Literal, TypeAlias, Union

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
