#!/usr/bin/env python3


import time
from functools import partial
from traceback import format_exception

from ..utils.logger import log_json
from .main import interrupt
from ..executor.execute_tool import ToolsManager


@interrupt("WAIT")
def wait_for_rate_limit():
    print("Sleeping !!!")
    time.sleep(12)

@interrupt("execute_cronjob")
def execute_cronjob(cronjob: partial[None]):
    try:
        cronjob()
        log_json({
            "type": 'cronjob',
            'status': 'normal',
            'cronjob': str(cronjob.func.__name__)
            })
    except Exception as e:
        log_json({
            'type': 'cronjob',
            'status': 'fatal',
            'cronjob': str(cronjob.func.__name__),
            'error': str(e),
            'traceback': str(format_exception(e))
        })
        print(f"CRONJOB {cronjob.func.__name__} failed for reason {e} with traceback {format_exception(e)}")


@interrupt("invalidate_tool_cache")
def tool_cache_invalidation(tool: str):
    tool_manager = ToolsManager()
    tool_manager.invalidate(tool)
