#!/usr/bin/env python3
"""
The file where all the event recievers are going to be located in.
"""
from typing import AsyncGenerator, Callable, TypeVar

from asyncinotify import Inotify, Mask

from ..utils.config_handlers import load_events_config
from ..utils.logger import log_json
from .event_gens_registry import register_event_generator
from .types import Event

event_config = load_events_config()

P = TypeVar('P')

def build_event(*parts: P, payload: str, converter: Callable[[P], str]) -> Event:
    """
    Constructs the event from the parts.
    The converter function is used on the parts to convert them to actual parts.
    We assume parts DONT have separators between eachother, so we '.'.join() them.

    !!!All the event paths are always made full lowercase!!!
    """
    
    parts_new: list[str] = []

    for i in parts: 
        parts_new.append(converter(i))

    event_path = '.'.join(parts_new)
    event_path.lower()
    return Event(event_path, payload)
    

@register_event_generator("filesystem.fanotify")
async def filesystem_gen() -> AsyncGenerator[Event, None]:
    """
    Event structure: filesystem.path.to.file.delete|create|modify .
    """

    def converter(input: str) -> str:
        """ Converts the path string representation to the NATS event path representation. """
        return input.replace('/', '.').removeprefix('.')

    with Inotify() as inotify:
        for p in event_config.filesystem_watch_dirs:
            inotify.add_watch(p, Mask.DELETE|Mask.CREATE|Mask.MODIFY)
        async for event in inotify:
            if event.path is None:
                log_json({
                    "type": "event",
                    'subtype': 'gen',
                    'function': 'filesystem_gen',
                    'status': 'error',
                    'message': 'event.path is None.'
                })
                continue
            if event.mask.name is None:
                log_json({
                    "type": "event",
                    'subtype': 'gen',
                    'function': 'filesystem_gen',
                    'status': 'error',
                    'message': 'event.name is None.'
                })
                continue
            yield build_event('filesystem', event.path.as_posix(), event.mask.name, payload='', converter=converter)
