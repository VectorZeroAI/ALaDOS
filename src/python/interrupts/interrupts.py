#!/usr/bin/env python3

from .main import interrupt
import time

@interrupt("WAIT")
def wait_for_rate_limit():
    print("Sleeping !!!")
    time.sleep(12)


@interrupt("execute_cronjob")
def execute_cronjob():
    ...
