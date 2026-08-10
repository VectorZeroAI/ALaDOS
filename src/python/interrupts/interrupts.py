#!/usr/bin/env python3


import time
from functools import partial
from traceback import format_exception

from ..executor.types import JsonSerializable, _ExecToolMetaData
from ..executor.execute_tool import execute_tool
from ..executor.types import ToolCall

from ..utils.logger import log_json
from .main import interrupt


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

@interrupt("syscall")
def execute_syscall(syscall: str, args: dict[str, JsonSerializable]):
    """
    Executes the syscall. Works together with base_state/custom_consumers.
    """
    meta = args.pop("_meta")
    if not isinstance(meta, _ExecToolMetaData):
        log_json({
            "type": "syscall",
            "subtype": "execution interrupt",
            "status": "fatal",
            "msg": f"META OF WRONG TYPE OR NOT PROVIDED. Got {meta} with type {type(meta)}, expected _ExecToolMetaData"
        })
    execute_tool(ToolCall(syscall, args), meta)
