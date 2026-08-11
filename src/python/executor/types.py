#!/usr/bin/env python3
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Literal, Sequence, TypeAlias, Union, get_args, Any, Callable

from nats.aio.client import Client
from pydantic import JsonValue

from ..types import ReferenceTo, SyscallsQueue, ToolCall
from ..utils.conn_factory import Conn
from ..utils.uqueue import Uqueue
from .exceptions import ContextLimitExceededError, ParadoxDetected

JsonSerializable: TypeAlias = JsonValue

SlaveScope: TypeAlias = Literal['all', 'general', 'context', 'task', 'communication']
SlaveScope_: TypeAlias = Literal[*get_args(SlaveScope), '_webui'] # pyright: ignore

SlaveScopesList: TypeAlias = Sequence[SlaveScope]

@dataclass(slots=True)
class Api:
    """ An api endpoint representation """
    url: str
    key: str
    model: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    rate_limited_until: float = 0.0
    consecutive_ratelimits: int = 0
    claude: bool = False
    max_tokens: int = 8000

@dataclass(slots=True)
class Instr:
    """ An atomic instruction json representation """
    result_addr: int
    instruction: str
    master_addr: int
    context: str
    slave_addr: int
    scope: SlaveScope_

ToolCallsBlock: TypeAlias = list[ToolCall]


@dataclass(slots=True)
class _ExecToolMetaData:
    """ Typed dict for the metadata transfer to the executed tools. """
    master_id: int
    conn: Conn 
    slave_id: int
    context_limit: int
    occ_last_change: datetime
    syscalls_queue: SyscallsQueue
    nats: Client
    _embedder_queue: Uqueue[ReferenceTo] = field(default_factory=Uqueue[ReferenceTo])


CachedTool: TypeAlias = Callable[[dict[str, Any], _ExecToolMetaData], str]

class Cs(Enum):
    GET_SLAVE = auto()
    CONTEXT_GEN = auto()
    API_CALLS = auto()
    EXECUTE = auto()
    CONTEXT_SHORTENING = auto()
    PARADOX = auto()
    ERROR = auto()
    FINISH = auto()


@dataclass(slots=True)
class GetSlaveState:
    tag: Literal[Cs.GET_SLAVE] = Cs.GET_SLAVE

@dataclass(slots=True)
class ContextGetState:
    slave_addr: ReferenceTo
    finish: bool
    tag: Literal[Cs.CONTEXT_GEN] = Cs.CONTEXT_GEN

@dataclass(slots=True)
class ApiCallsState:
    str_instr: str
    instr: Instr
    occ_timestamp: datetime
    finish: bool
    tag: Literal[Cs.API_CALLS] = Cs.API_CALLS

@dataclass(slots=True)
class ExecuteState:
    tool_calls: ToolCallsBlock
    instr: Instr
    occ_timestamp: datetime
    finish: bool
    error_count: int = 0
    tag: Literal[Cs.EXECUTE] = Cs.EXECUTE

@dataclass(slots=True)
class ContextShortState:
    slave_addr: ReferenceTo
    error: ContextLimitExceededError
    instr: Instr
    finish: bool
    tag: Literal[Cs.CONTEXT_SHORTENING] = Cs.CONTEXT_SHORTENING

@dataclass(slots=True)
class ParadoxState:
    paradox_e: ParadoxDetected
    instr: Instr
    time: datetime
    finish: bool
    tag: Literal[Cs.PARADOX] = Cs.PARADOX

@dataclass(slots=True)
class ErrorState:
    slave_addr: ReferenceTo
    tag: Literal[Cs.ERROR] = Cs.ERROR

@dataclass(slots=True)
class FinishState:
    results: list[str]
    metadata_c: _ExecToolMetaData
    instr: Instr
    tag: Literal[Cs.FINISH] = Cs.FINISH

State = Union[GetSlaveState, ContextGetState, ApiCallsState, ExecuteState, ContextShortState, ParadoxState, ErrorState, FinishState]
