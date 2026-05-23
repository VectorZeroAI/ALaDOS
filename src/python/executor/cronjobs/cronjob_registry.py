#!/usr/bin/env python3

from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ...executor.cronjobs.main import SysState

CRONJOB_REGISTRY = {}

def register_cronjob(name: str) -> Callable:
    def decorator(func: Callable):
        CRONJOB_REGISTRY[name or func.__name__] = func
        return func
    return decorator

def execute_cronjob(name: str, sys_state: SysState) -> Any:
    return CRONJOB_REGISTRY[name](sys_state)
