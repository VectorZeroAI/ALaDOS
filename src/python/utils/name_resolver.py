#!/usr/bin/env python3

from traceback import format_exception
from typing import Iterable

from python.executor.types import Conn
from python.types import ReferenceTo

from .logger import log_json

def resolve_to_addr(item: ReferenceTo|str, conn: Conn) -> ReferenceTo:
    """
    Tries to resolve an items name if its name.
    Always returns address, raises RuntimeError if no address found.
    """
    if isinstance(item, str):
        try:
            return conn.execute_fetchval("SELECT resolve_name(%s)", (item,))
        except Exception as e:
            log_json({
                'type': 'util',
                'subtype': 'name_resolver',
                'status': 'fatal',
                'error': str(e),
                'traceback': str(format_exception(e))
            })
            raise RuntimeError(f"resolution failed due to {e}")
    else:
        return item
    

def resolve_to_addrs(names_and_addrs: Iterable[ReferenceTo|str], conn: Conn) -> list[ReferenceTo]:
    """
    Resolved the the strings of a list into the numeric addressess.
    Raises RuntimeError if a name couldnt be resolved.
    """

    names_and_addrs = list(names_and_addrs)
    str_deps: list[str] = []
    int_deps: list[ReferenceTo] = []

    for i in names_and_addrs:
        if isinstance(i, str):
            str_deps.append(i)
        else:
            int_deps.append(i)

    try:
        addrs = conn.executemany("SELECT resolve_name(%s)", [(i,) for i in str_deps], returning=True)
        addrs = [a[0] for a in addrs]
    except Exception as e:
        log_json({
            'type': 'util',
            'subtype': 'name_resolver',
            'status': 'fatal',
            'error': str(e),
            'traceback': str(format_exception(e))
        })
        raise RuntimeError(f"Resolution failed with error {e}, because resolve_name somehow let an None through, or something was wrong upstream")

    int_deps.extend(addrs)

    return int_deps 
