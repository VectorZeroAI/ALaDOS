#!/usr/bin/env python3

from collections import defaultdict

from ..interrupts.main import InterruptInvokation
from ..types import ReferenceTo, SyscallsQueue, SyscallsQueues
from ..utils.uqueue import Uqueue

executor_interrupt_queue = Uqueue[InterruptInvokation]()

executor_queue = Uqueue[ReferenceTo]()

embedder_queue = Uqueue[ReferenceTo]()

syscalls_queue_dict_per_slave: SyscallsQueues = defaultdict(SyscallsQueue)
