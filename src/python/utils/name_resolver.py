#!/usr/bin/env python3

from os import name
from traceback import format_exception
from typing import Iterable, Sequence

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

def resolve_self(slave_addr: ReferenceTo, names_and_addrs: Sequence[str|ReferenceTo], conn: Conn) -> list[str|ReferenceTo]:
    """
    Resolved the "self" string in the input list to the slaves result address, slave address required.
    Later possibly will be expanded to include self_master as well.

    Resolves the slaves result addr automatically from the given addr.
    """
    result_addr = conn.execute_fetchval("SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,))

    names_and_addrs = list(names_and_addrs)
    for i in range(len(names_and_addrs)):
        if names_and_addrs[i] == "self":
            names_and_addrs[i] = result_addr
    
    return names_and_addrs
