#!/usr/bin/env python3

from typing import Callable

from .main import interrupt
from ..utils.logger import log_json
from traceback import format_exception
import time

@interrupt("WAIT")
def wait_for_rate_limit():
    print("Sleeping !!!")
    time.sleep(12)


@interrupt("execute_cronjob")
def execute_cronjob(cronjob: Callable[[], None]):
    try:
        cronjob()
        log_json({
            "type": 'cronjob',
            'status': 'normal',
            'cronjob': str(cronjob.__name__)
            })
    except Exception as e:
        log_json({
            'type': 'cronjob',
            'status': 'fatal',
            'cronjob': str(cronjob.__name__),
            'error': str(e),
            'traceback': format_exception(e)
        })
        print(f"CRONJOB {cronjob.__name__} failed for reason {e} with traceback {format_exception(e)}")
