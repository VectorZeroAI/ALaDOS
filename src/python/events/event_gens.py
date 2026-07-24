#!/usr/bin/env python3
"""
The file where all the event recievers are going to be located in.
"""
from typing import AsyncGenerator

from asyncinotify import Inotify, Mask

from ..utils.config_handlers import load_events_config
from ..utils.logger import log_json
from .event_gens_registry import register_event_generator
from .types import Event

event_config = load_events_config()

@register_event_generator("filesystem.fanotify")
async def filesystem_gen() -> AsyncGenerator[Event, None]:
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
            yield Event('filesystem.' + event.mask.name + event.path.as_posix(), '')
