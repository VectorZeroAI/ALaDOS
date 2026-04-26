#!/usr/bin/env python3
from __future__ import annotations

from typing import TypeAlias, TypedDict, Optional, List
import psycopg
from pydantic import JsonValue
from python.utils.uqueue import Uqueue

JsonSerializable: TypeAlias = JsonValue

addr: TypeAlias = int

class api(TypedDict, total=False):
    """ An api endpoint representation """
    url: str
    key: str
    model: str
    claude: Optional[bool]
    max_tokens: Optional[int]
    rate_limited_until: float
    consecutive_ratelimits: int

class instr_json(TypedDict):
    """ An atomic instruction json representation """
    result_addr: int
    instruction: str
    master_addr: int
    context: str

class tool_call(TypedDict, total=False):
    """ A single tool call, directly executable """
    tool: str
    args: Optional[dict[str, JsonSerializable]]

tool_calls_block: TypeAlias = List[tool_call]

class _exec_tool_meta_data(TypedDict):
    """ Typed dict for the metadata transfer to the executed tools. """
    master_id: int
    conn: psycopg.Connection
    _embedder_queue: Uqueue
