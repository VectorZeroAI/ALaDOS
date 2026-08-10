#!/usr/bin/env python3
"""
The implementation of the lib handshake structure.

The structure:
    event send to _.syscall.request.<insert_slave_id_here>.<insert_function_name_here>
    recieve responce from _.syscall.responce.<insert_slave_id_here>.<insert_function_name_here>

Batching results in like "send 50 then recieve all 50" theoretically allowed.
"""

from ..types import CustomConsumer
from ..registry import register
from ...events.types import Event
from ...executor.queue import executor_interrupt_queue
from ...interrupts.main import InterruptInvokation
import json

def callback(event: Event):
    syscall_name = event.event_path.split('.')[-1]
    return_event_path = event.event_path.replace(".request.", ".responce.")

    executor_interrupt_queue.put(
        InterruptInvokation(
            "syscall",
            {
                "function": syscall_name,
                "args": json.loads(event.payload),
                "return_to": return_event_path
            }
        )
    )

CustomConsumer(
        event_path="_.syscall.request.*.*",
        consumer_inner_callback=callback
)
