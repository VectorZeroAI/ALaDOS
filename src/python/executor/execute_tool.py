#!/usr/bin/env python3

from typing import Callable
from python.executor.types import tool_call
import inspect
import re
import psycopg

TOOL_REGISTRY = {}
HEADERS_REGISTRY = {}

TOOL_USAGE_INSTRUCTION = """
You must ALWAYS call tools, not outputting tool calls is FORBIDDEN.
When calling tools you must follow this instruction format:
[
    {
        "tool": "tool.name",
        "args": {
            "param_name": "value",
            "anouther_param_name": 123
        }

    },
    {
        ...
    },
    ...
]
"""

HEADERS_REGISTRY[" "] = TOOL_USAGE_INSTRUCTION # TODO : Maybe make this a bit nicer, IDK, maybe

# Pattern matches:
# - optional comma and whitespace before (if not first param)
# - the parameter itself: _master_addr: <type>
# - optional default value like = ...
# - optional trailing comma if it was the last param
pattern_master_id = r'(?:,\s*)?_master_id\s*:\s*[^,=)]+(?:\s*=\s*[^,)]+)?(?:,\s*)?'
pattern_conn = r'(?:,\s*)?_conn\s*:\s*[^,=)]+(?:\s*=\s*[^,)]+)?(?:,\s*)?'

def remove_master_addr_param(signature_str: str) -> str:
    tmp = re.sub(pattern_master_id, '', signature_str).strip()
    tmp = re.sub(pattern_conn, '', tmp).strip()
    return tmp


def _construct_header(func: Callable, name: str|None = None) -> str:
    signature = inspect.signature(func)
    signature_str = name or func.__name__
    signature_str = signature_str + str(signature)
    signature_str = remove_master_addr_param(signature_str)
    signature_str = "\n".join((signature_str, (func.__doc__ or "No description provided")))
    return signature_str


def register_tool(name: str|None = None):
    def decorator(func: Callable):
        TOOL_REGISTRY[name or func.__name__] = func
        HEADERS_REGISTRY[name or func.__name__] = _construct_header(func, name)
        return func
    return decorator

def execute_tool(call: tool_call, _master_id: int, _conn: psycopg.Connection) -> None:
    return TOOL_REGISTRY[call["tool"]](**call.get("args", {}), _master_id = _master_id, _conn = _conn) # pyright: ignore

# register all the tools
from . import builtins # # pyright: ignore # ruff: ignore 
# THIS IS REQUIRED ! DONT REMOVE THIS!!!
