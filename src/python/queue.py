#!/usr/bin/env python3

from .interrupts.main import InterruptInvokation

from .utils.uqueue import Uqueue

global_interrupt_queue = Uqueue[InterruptInvokation]()
