#!/usr/bin/env python3

from .main import interrupt
import time

@interrupt("WAIT")
def wait_for_rate_limit():
    time.sleep(12)
