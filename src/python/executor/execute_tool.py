#!/usr/bin/env python3

import traceback
from typing import Callable, ParamSpec, TypeAlias, TypeVar, get_args

from python.types import ReferenceTo
from ..executor.types import ToolCall
import inspect
import re
from .types import _ExecToolMetaData, JsonSerializable, SlaveScope_, SlaveScopesList, ToolCallsBlock

IdExtractor: TypeAlias = Callable[[dict[str, JsonSerializable]], ReferenceTo|None]

TOOL_REGISTRY: dict[str, Callable] = {}
HEADERS_REGISTRY: dict[str, str] = {}
IDS_ARGNAMES: dict[str, list[str]] = {}

TOOL_USAGE_INSTRUCTION = """
You should output tool calls. Otherwise your response will be treated as plaintext result. 
Whenever you see the argument id, it means its ether an address, e.g. number, or a name.
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

P = ParamSpec('P')
R = TypeVar('R')


def register_tool(name: str|None = None, scope: SlaveScopesList = ['general'], id_names: list[str] = []):
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        TOOL_REGISTRY[name or func.__name__] = func
        IDS_ARGNAMES[name or func.__name__] = id_names
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



def extract_ids(tool_calls: ToolCallsBlock) -> list[ReferenceTo]:
    """
    Extracts the addrs that will be edited by the functions and returns them in a list.
    Used for OCC in core. 
    """
    addrs = []
    try:
        for i in tool_calls:
            addr_ids = IDS_ARGNAMES[i.tool]
            for j in addr_ids:
                candidate = i.args[j]
                if not isinstance(candidate, int):
                    raise ValueError(f"ONE OF THE IDS IS NOT INT, gotten {candidate} from args {i.args} from tool {i.tool}, expected int.")
                addrs.append(candidate)
    except KeyError as e:
        raise ValueError(f"KEY ERROR {e} with traceback {traceback.format_exception(e)}.")
    return addrs


# register all the tools
# THIS IS REQUIRED ! DONT REMOVE THIS!!!
from . import builtins as __owuergnsorjgnborn  # noqa # pyright: ignore
