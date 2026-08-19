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
from ...utils.conn_factory import conn_factory

from ...executor.types import (
    ToolCallsBlock,
    _ExecToolMetaData,
    syscalls_json_db_bulk_format,
)
from ...types import SysCall

conn = conn_factory() # TODO: add retries. 

def new_execution(tool_calls: ToolCallsBlock, meta: _ExecToolMetaData) -> None:
    """
    Inits the metadata_dag tracing by inserting the starting data to be then later appended to.
    
    Execution
    """
    conn.execute("""
    INSERT INTO metadata_dag(addr_s, metadata) VALUES (%s, %s)
                 """,
                (
                    meta.slave_addr,
                    Jsonb({
                        "tool_calls": tool_calls
                    })
                )
    )


def bulk_append_syscalls_to_trace(meta: _ExecToolMetaData, syscalls: list[SysCall]) -> None:
    """
    Bulk appends syscalls to the DB trace of execution, for _execute_tool.
    """
    array_for_db: syscalls_json_db_bulk_format = [
        {"syscall": s.tool, "args": s.args} for s in syscalls
    ]

    conn.execute("""
    SELECT concat_syscalls_to_metadata_dag(%s, %s)
                 """, (meta.slave_addr, array_for_db))

def tool_errored(error: str, meta: _ExecToolMetaData) -> None:
    """
    Register tool error from ExecuteState, for core.
    """
    raise NotImplementedError("TOOL ERRORED TRACING HOOK FOR CORE NOT YET IMPLEMENTED")

def execution_aborted(error: str, meta: _ExecToolMetaData) -> None:
    """
    The hook for tracing the abortion of execution of the core due to error.
    """
    raise NotImplementedError("EXECUTION ABORTED TRACING HOOK FOR CORE NOT YET IMPLEMENTED"):
        
