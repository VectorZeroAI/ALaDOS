#!/usr/bin/env python3
"""
Cores tracing file.
Contains all the functions used by the core to store execution traces.

The traces are to not be rolled back the transactions of core,
so this file will use its own connection.

Also optimiser could include analysing reocurring error patterns and
prompting improvement of function descriptions and or headers.
"""


from psycopg.types.json import Jsonb

from ...executor.types import (
    _ExecToolMetaData,
)
from ...types import ToolCall
from .main import conn


def tool_executing(meta: _ExecToolMetaData, tool_call: ToolCall) -> None:
    """
    Traces the tool call.
    """
    conn.execute("""
    SELECT tool_executing_trace(%s, %s, %s);
                 """, (meta.slave_addr, tool_call.called_id, Jsonb(tool_call.args)))


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
