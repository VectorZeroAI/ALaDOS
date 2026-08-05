#!/usr/bin/env python3

from typing import Any, Callable, TYPE_CHECKING, ParamSpec
from functools import partial

if TYPE_CHECKING:
    from ...executor.cronjobs.main import SysState

P = ParamSpec('P')

CRONJOB_REGISTRY: dict[str, Callable[..., None]] = {}

def register_cronjob(name: str):
    """
    Cronjob patterns documentation:
        All the cronjobs should take in **kwargs and access their inputs through them.
    Should they? Ill add an Issue on that. 
    """ # TODO : Add an issue on that.
    def decorator(func: Callable[P, None]) -> Callable[P, None]:
        CRONJOB_REGISTRY[name or func.__name__] = func
        return func
    return decorator

def execute_cronjob(name: str, sys_state: SysState, args: dict[str, Any]) -> None:
    args['sys_state'] = sys_state
    CRONJOB_REGISTRY[name](**args)

def prepare_cronjob(name: str, sys_state: SysState, args: dict[str, Any]) -> Callable[[], None]:
    args['sys_state'] = sys_state
    return partial(CRONJOB_REGISTRY[name], **args)
