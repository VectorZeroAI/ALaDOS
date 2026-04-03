#!/usr/bin/env python3

from typing import TypeAlias, TypedDict, Optional, Union

from pydantic import BaseModel

JsonSerialisable: TypeAlias = Union[str, int, float, bool, None, dict[str, "JsonSerialisable"], list["JsonSerialisable"]]

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
    args: dict[str, JsonSerialisable]

class tool_calls_block(TypedDict):
    """ The full tool block class, directly extracted from LLM output """
    tool_calls: list[tool_call]
