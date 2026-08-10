#!/usr/bin/env python3

from collections import defaultdict

from ..executor.types import ToolCall
from ..interrupts.main import InterruptInvokation
from ..types import ReferenceTo
from ..utils.uqueue import Uqueue

executor_interrupt_queue = Uqueue[InterruptInvokation]()

executor_queue = Uqueue[ReferenceTo]()

embedder_queue = Uqueue[ReferenceTo]()

syscalls_queue_dict_per_slave: dict[ReferenceTo, Uqueue[tuple[ToolCall, str]]] = defaultdict(Uqueue[tuple[ToolCall, str]])

"""
The string, e.g. tuple[2] of syscalls_quue_dict_per_slave is the return event_path.
Although it can be fully reconstructed, I still choose to send it over.
Maybe in the future I will change that.
"""
