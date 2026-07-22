#!/usr/bin/env python3

from types import FunctionType
import functools
from typing import Any
from dataclasses import dataclass, field

from ..utils.uqueue import Uqueue

@dataclass(slots=True, frozen=True)
class InterruptInvokation:
    name: str
    args: dict[str, Any] = field(default_factory=dict)

INTERRUPT_TABLE: dict[str, FunctionType] = {}

def interrupt(name: str|None = None) -> FunctionType:
    """
    register a new interrupt into the interrupt system.
    Overwrites if the interrupt was present before the operation.
    """
    def decorator(func: FunctionType):
        INTERRUPT_TABLE[name or func.__name__] = func
        return func
    return decorator

def interruptable(*q: Uqueue[InterruptInvokation]) -> FunctionType:
    """
    @interruptible(queue1, queue2)
    """
    def decorator(input_func):
        def checkpoint() -> None: 
            while True:
                interrupt_found = False
                for i in q:
                    interrupt = i.get_nowait()
                    
                    if not interrupt:
                        continue

                    interrupt_found = True
                    interrupt_handler = INTERRUPT_TABLE.get(InterruptInvokation.name)
                    if interrupt_handler:
                        interrupt_handler(**InterruptInvokation.args)
                if not interrupt_found:
                    break

        
        @functools.wraps(input_func)
        def wrapper(*args, **kwargs):
            return input_func(checkpoint, *args, **kwargs)
        return wrapper
    return decorator

from ..interrupts import interrupts as _srgiusbeftsdrgfb # NOTE : DONT FUCKING TOUCH! #noqa #pyright: ignore
