#!/usr/bin/env python3
"""
This file is the registry for the event recievers.

YOU can write your event generators in any language and just wire them into the NATS server. 
You should also simply document them in a knowledge item and thats it.
Or you can of course write one in python and get the benefits of automatic integration,
for the propable performance issues if there are way to many events, so you gotta be carefull.
"""

from typing import AsyncGenerator, Callable, Coroutine

from ..events.types import Event
from ..utils.logger import log_json

EVENT_PRODUCERS: list[Coroutine[None, None, None]] = []
EVENT_DOCS: str = ""

def register_event_generator(name: str):
    """
    The decorator to register the event generator.
    """
    def decorator(func: Callable[[], AsyncGenerator[Event, None]]) -> Callable[[], AsyncGenerator[Event, None]]:
        async def producer() -> None:
            async for event in func():
                await event.send()

        global EVENT_DOCS

        EVENT_PRODUCERS.append(producer())
        if func.__doc__ is None:
            EVENT_DOCS = EVENT_DOCS + f"\n\n EVENT GENERATOR {func.__name__} DOESNT HAVE DOCUMENTATION"
            log_json({
                "type": "event",
                "subtype": "event_gens",
                "status": "warning",
                "msg": f"EVENT GENERATOR {func.__name__} DOESNT HAVE DOCUMENTATION!!!"
            })
            return func

        EVENT_DOCS = EVENT_DOCS + "\n\n" + func.__doc__
        return func

    return decorator

# TODO : make printout nicer in more places using this kind of technique.
