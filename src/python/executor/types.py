#!/usr/bin/env python3
from __future__ import annotations

from typing import Literal, Sequence, TypeAlias, TypedDict, Optional
import psycopg
from pydantic import JsonValue
from ..utils.uqueue import Uqueue

JsonSerializable: TypeAlias = JsonValue

Addr: TypeAlias = int

SlaveScope: TypeAlias = Literal['all', 'general', 'context', 'task', 'communication']
SlaveScope_: TypeAlias = SlaveScope|Literal['_webui']

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
    conn: psycopg.Connection
    _embedder_queue: Uqueue
    slave_id: int
    context_limit: int
