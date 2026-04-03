#!/usr/bin/env python3

from typing import Callable
from python.executor.types import tool_call

TOOL_REGISTRY = {}

def register_tool(name: str|None = None):
    def decorator(func: Callable):
        TOOL_REGISTRY[name or func.__name__] = func
        return func
    return decorator

def execute_tool(call: tool_call) -> None:
    
