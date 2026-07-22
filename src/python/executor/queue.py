#!/usr/bin/env python3

from ..interrupts.main import InterruptInvokation
from ..types import ReferenceTo
from ..utils.uqueue import Uqueue

executor_interrupt_queue = Uqueue[InterruptInvokation]()

executor_queue = Uqueue[ReferenceTo]()

embedder_queue = Uqueue[ReferenceTo]()
