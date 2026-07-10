#!/usr/bin/env python3

import asyncio
from types import FunctionType
import functools

from ..utils.uqueue import Uqueue

INTERRUPT_TABLE = {}

def interrupt(name: str|None = None) -> FunctionType:
    """
    register a new interrupt into the interrupt system.
    Overwrites if the interrupt was present before the operation.
    """
    def decorator(func: FunctionType):
        INTERRUPT_TABLE[name or func.__name__] = func
        return func
    return decorator

def interruptable(*q: Uqueue[str]) -> FunctionType:
    """
    @interruptible(queue1, queue2)
    """
    def decorator(input_func):

        def checkpoint() -> None: 
            while True:
                for i in q:
                    interrupt = i.get_nowait()
                    
                    if not interrupt:
                        break

                    interrupt_handler = INTERRUPT_TABLE.get(interrupt)
                    if interrupt_handler:
                        interrupt_handler()

        
        @functools.wraps(input_func)
        def wrapper(*args, **kwargs):
            return input_func(checkpoint, *args, **kwargs)
        return wrapper
    return decorator

from ..interrupts import interrupts as _srgiusbeftsdrgfb # NOTE : This is valid and works. Dont ask why. No one knows (python quirks)
