#!/usr/bin/env python3
from __future__ import annotations

from typing import TypeAlias, TypedDict, Optional, Union, List
from pydantic import Json

JsonSerializable: TypeAlias = Json

class api(TypedDict):
    """ An api endpoint representation """
    url: str
    key: str
    model: str
    claude: Optional[bool]

class instr_json(TypedDict):
    """ An atomic instruction json representation """
    result_addr: int
    instruction: str
    master_addr: int
    context: str

class tool_call(TypedDict):
    """ A single tool call, directly executable """
    tool: str
    args: dict[str, JsonSerializable]

tool_calls_block: TypeAlias = List[tool_call]
