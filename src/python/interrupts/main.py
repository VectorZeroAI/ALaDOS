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
        if isinstance(input_func, type):
            raise TypeError("@interruptible cannot be used on classes.")
        if not asyncio.iscoroutinefunction(input_func):
            raise TypeError("@interruptible requires an async function.")

        
        @functools.wraps(input_func)
        async def wrapper(*args, **kwargs):
            pause_event = asyncio.Event()
            pause_event.set()

            async def checkpoint():
                """ The point at wich the function is interruptable by an interrupt. """
                await pause_event.wait()

            async def listen_to(q: Uqueue[str]):
                while True:
                    name = await q.get()
                    pause_event.clear()
                    interrupt_handler = INTERRUPT_TABLE.get(name)
                    if interrupt_handler:
                        if asyncio.iscoroutinefunction(interrupt_handler):
                            print(f"Interrupt {name} executed")
                            await interrupt_handler()
                        else:
                            await asyncio.to_thread(interrupt_handler)
                    else:
                        print(f"Unknown interrupt {name} used")
                    pause_event.set()
    
            listeners = [asyncio.create_task(listen_to(que)) for que in q]
            try:
                return await input_func(checkpoint, *args, **kwargs)
            finally:
                for i in listeners:
                    i.cancel()
        return wrapper
    return decorator

from ..interrupts import interrupts as _srgiusbeftsdrgfb # NOTE : WHy does this not register the interrupt?!!?!?!?!?!?
