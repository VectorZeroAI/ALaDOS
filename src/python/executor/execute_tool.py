#!/usr/bin/env python3

from typing import Callable
from python.executor.types import tool_call
import inspect
import re

TOOL_REGISTRY = {}
HEADERS_REGISTRY = {}

# Pattern matches:
# - optional comma and whitespace before (if not first param)
# - the parameter itself: _master_addr: <type>
# - optional default value like = ...
# - optional trailing comma if it was the last param
pattern = r'(?:,\s*)?_master_id\s*:\s*[^,=)]+(?:\s*=\s*[^,)]+)?(?:,\s*)?'

def remove_master_addr_param(signature_str: str) -> str:
    return re.sub(pattern, '', signature_str).strip()


def _construct_header(func: Callable) -> str:
    signature = inspect.signature(func)
    signature_str = str(signature)
    signature_str = remove_master_addr_param(signature_str)
    signature_str = "\n".join((signature_str, (func.__doc__ or "No description provided")))
    return signature_str


def register_tool(name: str|None = None):
    def decorator(func: Callable):
        TOOL_REGISTRY[name or func.__name__] = func
        HEADERS_REGISTRY[name or func.__name__] = _construct_header(func)
        return func
    return decorator

def execute_tool(call: tool_call, _master_id: int) -> None:
    return TOOL_REGISTRY[call["tool"]](**call["args"], _master_id = _master_id)
