#!/usr/bin/env python3
"""
Base state.

Base state is basically the system provided knowledge entires, rmts, tools, etc.
The system enforces that they are present at startup,
and the system generally disallows changing them.

It is of course bypassable if you really need to change them,
but you cant delete them, as they will be forced back into existance at next startup.

There is a meta toolkit for defining those in the registry.py file in this directory. 
main.py handles the enforcement at startup time,
and then there is a directory full of files that are the actual definitions. 
"""

from typing import Coroutine

from ..utils.conn_factory import conn_factory
from ..utils.logger import log_json
from .registry import SYSTEM_ADDRS_LIST, ADDR_REGISTER, CUSTOM_CONSUMERS
from traceback import format_exception

def startup() -> list[Coroutine[None, None, None]]:
    conn = conn_factory()

    results = conn.execute("""
    SELECT unnest(%s::BIGINT[])
    EXCEPT
    SELECT addr FROM addrs;
                           """, (SYSTEM_ADDRS_LIST,)).fetchall()

    results = [r[0] for r in results]

    from . import state_components # noqa # pyright: ignore

    for i in results:
        try:
            ADDR_REGISTER[i]()
        except Exception as e:
            log_json({
                "type": "base_state",
                "subtype": "ADDR_REGISTER",
                "msg": str(e),
                "traceback": str(format_exception(e)),
                "context": f"Addr = {i}"
            })

    print("Base state finished.")
    return CUSTOM_CONSUMERS

