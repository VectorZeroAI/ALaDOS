#!/usr/bin/env python3
"""
Cores tracing file.
Contains all the functions used by the core to store execution traces.

The traces are to not be rolled back the transactions of core,
so this file will use its own connection.

Also optimiser could include analysing reocurring error patterns and
prompting improvement of function descriptions and or headers.
"""

from ...executor.types import (
    _ExecToolMetaData,
    syscalls_json_db_bulk_format,
)
from ...types import SysCall
from .main import conn


def new_execution(meta: _ExecToolMetaData) -> None:
    """
    Inits the metadata_dag tracing by inserting the starting data to be then later appended to.
    
    Execution
    """
    conn.execute("""
    SELECT new_execution_trace(%s);
                 """, (meta.slave_addr,))




def tool_errored(error: str, meta: _ExecToolMetaData) -> None:
    """
    Register tool error from ExecuteState, for core.
    """
    conn.execute("""
    SELECT tool_errored_trace(%s, %s);
                 """, (error, meta.slave_addr))


def execution_aborted(error: str, meta: _ExecToolMetaData) -> None:
    """
    The hook for tracing the abortion of execution of the core due to error.
    """
    conn.execute("""
    SELECT execution_aborted_trace(%s, %s)
                 """, (error, meta.slave_addr))
