#!/usr/bin/env python3
from __future__ import annotations

from typing import Literal, Optional, Sequence, TypeAlias, TypedDict, get_args, Union
from enum import Enum, auto
from dataclasses import dataclass

from pydantic import JsonValue

from ..utils.conn_factory import Conn
from ..utils.uqueue import Uqueue
from ..types import ReferenceTo
from .exceptions import ParadoxDetected, ContextLimitExceededError

JsonSerializable: TypeAlias = JsonValue

Addr: TypeAlias = int

SlaveScope: TypeAlias = Literal['all', 'general', 'context', 'task', 'communication']
SlaveScope_: TypeAlias = Literal[*get_args(SlaveScope), '_webui'] # pyright: ignore

SlaveScopesList: TypeAlias = Sequence[SlaveScope]

class Api(TypedDict, total=False):
    """ An api endpoint representation """
    url: str
    key: str
    model: str
    claude: Optional[bool]
    max_tokens: Optional[int]
    rate_limited_until: float
    consecutive_ratelimits: int

class InstrJson(TypedDict):
    """ An atomic instruction json representation """
    result_addr: int
    instruction: str
    master_addr: int
    context: str
    slave_addr: int
    scope: SlaveScope_

class ToolCall(TypedDict, total=False):
    """ A single tool call, directly executable """
    tool: str
    args: Optional[dict[str, JsonSerializable]]

ToolCallsBlock: TypeAlias = list[ToolCall]

class _ExecToolMetaData(TypedDict):
    """ Typed dict for the metadata transfer to the executed tools. """
    master_id: int
    conn: Conn 
    _embedder_queue: Uqueue
    slave_id: int
    context_limit: int

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
    instr: InstrJson
    finish: bool = False
    tag: Literal[Cs.API_CALLS] = Cs.API_CALLS

@dataclass(slots=True)
class ExecuteState:
    tool_calls: ToolCallsBlock
    instr: InstrJson
    error_count: int = 0
    finish: bool = False
    tag: Literal[Cs.EXECUTE] = Cs.EXECUTE

@dataclass(slots=True)
class ContextShortState:
    slave_addr: ReferenceTo
    error: ContextLimitExceededError
    instr: InstrJson
    tag: Literal[Cs.EXECUTE] = Cs.EXECUTE

@dataclass(slots=True)
class ParadoxState:
    paradox_e: ParadoxDetected
    instr: InstrJson
    tag: Literal[Cs.PARADOX] = Cs.PARADOX

@dataclass(slots=True)
class ErrorState:
    slave_addr: ReferenceTo
    tag: Literal[Cs.ERROR] = Cs.ERROR

@dataclass(slots=True)
class FinishState:
    results: list[str]
    metadata_c: _ExecToolMetaData
    instr: InstrJson
    tag: Literal[Cs.FINISH] = Cs.FINISH

State = Union[GetSlaveState, ContextGetState, ApiCallsState, ExecuteState, ContextShortState, ParadoxState, ErrorState, FinishState]
