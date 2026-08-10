#!/usr/bin/env python3
"""
The implementation of the lib handshake structure.

The structure:
    request sent to _.syscall.<slave_addr>.<tool_name>

Batching results in like "send 50 then recieve all 50" theoretically allowed.
"""

import json

from nats.aio.client import Client
from nats.aio.msg import Msg

from ...events.types import Event
from ...executor.queue import syscalls_queue_dict_per_slave
from ...executor.types import ToolCall
from ..registry import register
from ..types import CustomConsumer


def callback(msg: Msg, nats: Client):
    syscall_name = msg.subject.split('.', 3)[-1]
    slave_addr = int(msg.subject.split('.', 3)[-2]) # FIXME : Rename stuff so that Names of syscalls dont contain dots. 
    syscalls_queue_dict_per_slave[slave_addr].put(
        (
            ToolCall(
                tool=syscall_name,
                args=json.loads(msg.data.decode())
            ),
            msg
        )
    )

register(
    CustomConsumer(
        event_path="_.syscall.*.*",
        consumer_inner_callback=callback
    )
)
