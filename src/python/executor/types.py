#!/usr/bin/env python3
from __future__ import annotations

import threading
from typing import Literal, Sequence, TypeAlias, get_args, Union
from enum import Enum, auto
from dataclasses import dataclass, field

from pydantic import JsonValue

from ..utils.conn_factory import Conn
from ..utils.uqueue import Uqueue
from ..types import ReferenceTo
from .exceptions import ParadoxDetected, ContextLimitExceededError

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

@dataclass(slots=True)
class ToolCall:
    """ A single tool call, directly executable """
    tool: str
    args: dict[str, JsonSerializable] = field(default_factory=dict[str, JsonSerializable])

ToolCallsBlock: TypeAlias = list[ToolCall]

@dataclass(slots=True)
class _ExecToolMetaData:
    """ Typed dict for the metadata transfer to the executed tools. """
    master_id: int
    conn: Conn 
    slave_id: int
    context_limit: int
    _embedder_queue: Uqueue = field(default_factory=Uqueue[ReferenceTo])

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
    tag: Literal[Cs.CONTEXT_GEN] = Cs.CONTEXT_GEN

@dataclass(slots=True)
class ApiCallsState:
    str_instr: str
    instr: Instr
    finish: bool = False
    tag: Literal[Cs.API_CALLS] = Cs.API_CALLS

@dataclass(slots=True)
class ExecuteState:
    tool_calls: ToolCallsBlock
    instr: Instr
    error_count: int = 0
    finish: bool = False
    tag: Literal[Cs.EXECUTE] = Cs.EXECUTE

@dataclass(slots=True)
class ContextShortState:
    slave_addr: ReferenceTo
    error: ContextLimitExceededError
    instr: Instr
    tag: Literal[Cs.EXECUTE] = Cs.EXECUTE

@dataclass(slots=True)
class ParadoxState:
    paradox_e: ParadoxDetected
    instr: Instr
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
