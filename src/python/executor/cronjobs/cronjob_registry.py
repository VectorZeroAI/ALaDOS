#!/usr/bin/env python3

from typing import Any, Callable, TYPE_CHECKING
from functools import partial

if TYPE_CHECKING:
    from ...executor.cronjobs.main import SysState

CRONJOB_REGISTRY = {}

def register_cronjob(name: str) -> Callable:
    def decorator(func: Callable):
        CRONJOB_REGISTRY[name or func.__name__] = func
        return func
    return decorator

def execute_cronjob(name: str, sys_state: SysState, args: dict[str, Any]) -> None:
    args['sys_state'] = sys_state
    return CRONJOB_REGISTRY[name](**args)

def prepare_cronjob(name: str, sys_state: SysState, args: dict[str, Any]) -> Callable[[], None]:
    args['sys_state'] = sys_state
    return partial(CRONJOB_REGISTRY[name], **args)
