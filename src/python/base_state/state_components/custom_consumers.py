#!/usr/bin/env python3

from ..types import CustomConsumer
from ..registry import register
from ...events.types import Event
from ...executor.queue import executor_interrupt_queue
from ...interrupts.main import InterruptInvokation
import json

def callback(event: Event):
    syscall_name = event.event_path.split('.')[-1]
    executor_interrupt_queue.put(
        InterruptInvokation(
            "syscall",
            {
                "function": syscall_name,
                "args": json.loads(event.payload)
            }
        )
    )

CustomConsumer(
        event_path="_.syscall.*.*",
        consumer_inner_callback=callback
)
