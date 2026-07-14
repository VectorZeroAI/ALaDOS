#!/usr/bin/env python3

from typing import Callable, get_args
from ..executor.types import ToolCall
import inspect
import re
from .types import _ExecToolMetaData, SlaveScope_, SlaveScopesList

TOOL_REGISTRY = {}
HEADERS_REGISTRY = {}

TOOL_USAGE_INSTRUCTION = """
You should output tool calls. Otherwise your response will be treated as plaintext result. 
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

for i in get_args(SlaveScope_): # TODO : Maybe make this a bit nicer, IDK, maybe
    HEADERS_REGISTRY[i] = TOOL_USAGE_INSTRUCTION

# Pattern matches:
# - optional comma and whitespace before (if not first param)
# - the parameter itself: _master_addr: <type>
# - optional default value like = ...
# - optional trailing comma if it was the last param
pattern = r'(?:,\s*)?_meta\s*:\s*[^,=)]+(?:\s*=\s*[^,)]+)?(?:,\s*)?'

def remove_master_addr_param(signature_str: str) -> str:
    tmp = re.sub(pattern, '', signature_str).strip()
    return tmp


def _construct_header(func: Callable, name: str|None = None) -> str:
    signature = inspect.signature(func)
    signature_str = name or func.__name__
    signature_str = signature_str + str(signature)
    signature_str = remove_master_addr_param(signature_str)
    signature_str = "\n".join((signature_str, (func.__doc__ or "No description provided")))
    return signature_str


def register_tool(name: str|None = None, scope: SlaveScopesList = ['general'] ):
    def decorator(func: Callable):
        TOOL_REGISTRY[name or func.__name__] = func
        header = _construct_header(func, name)
        for i in scope:
            HEADERS_REGISTRY[i] = "\n\n".join([HEADERS_REGISTRY[i], header])
        HEADERS_REGISTRY['all'] = "\n\n".join([HEADERS_REGISTRY['all'], header])

        # Special internal thingis here.
        HEADERS_REGISTRY['_webui'] = HEADERS_REGISTRY['general']
        return func
    return decorator

def execute_tool(call: ToolCall, _meta: _ExecToolMetaData) -> str:
    return TOOL_REGISTRY[call.tool](**call.args, _meta = _meta)

# register all the tools
# THIS IS REQUIRED ! DONT REMOVE THIS!!!
from . import builtins as __owuergnsorjgnborn  # noqa # pyright: ignore
