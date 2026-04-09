#!/usr/bin/env python3

from typing import Callable
from python.executor.types import tool_call
import inspect

TOOL_REGISTRY = {}
HEADERS_REGISTRY = {}

def _construct_header(func: Callable) -> str:
    signature = inspect.signature(func)
    signature_str = signature.format()
    signature_str = "\n".join((signature_str, (func.__doc__ or "No description provided")))
    return signature_str


def register_tool(name: str|None = None):
    def decorator(func: Callable):
        TOOL_REGISTRY[name or func.__name__] = func
        HEADERS_REGISTRY[name or func.__name__] = _construct_header(func)
        return func
    return decorator

def execute_tool(call: tool_call) -> None:
    try:
        TOOL_REGISTRY[call["tool"]](**call["args"])
    except KeyError as e:
        pass
        # TODO : deside if all the tools should be loaded into TOOL_REGISTRY from the DB on startup, or if
        # they should be searched for in the DB each time a key error is faced. 
