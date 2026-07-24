#!/usr/bin/env python3
"""
This file is the registry for the event recievers.
"""

from typing import AsyncGenerator, Callable, Coroutine

from ..events.types import Event

EVENT_PRODUCERS: list[Coroutine[None, None, None]] = []

def register_event_generator(name: str):
    """
    The decorator to register the event generator.
    """
    def decorator(func: Callable[[], AsyncGenerator[Event, None]]) -> Callable[[], AsyncGenerator[Event, None]]:
        async def producer() -> None:
            async for event in func():
                event.send()

        EVENT_PRODUCERS.append(producer())
        return func

    return decorator
