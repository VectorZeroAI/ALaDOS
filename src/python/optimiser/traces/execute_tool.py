#!/usr/bin/env python3
"""
The file where the tracing components for execute_tool are located.
"""
from typing import Any

from psycopg.types.json import Jsonb

from ...executor.types import _ExecToolMetaData, syscalls_json_db_bulk_format
from ...types import ReferenceTo, SysCall
from .main import conn


def tool_executing(meta: _ExecToolMetaData, tool_id: ReferenceTo, args: dict[str, Any]) -> None:
    """
    Traces the tool call.
    """
    conn.execute("""
    SELECT tool_executing_trace(%s, %s, %s);
                 """, (meta.slave_addr, tool_id, Jsonb(args)))
    
def bulk_syscalls_trace(meta: _ExecToolMetaData, syscalls: list[SysCall]) -> None:
    """
    Bulk appends syscalls to the DB trace of execution, for _execute_tool.
    """
    array_for_db: syscalls_json_db_bulk_format = [
        {"syscall": s.called_id, "args": s.args} for s in syscalls
    ]

    conn.execute("""
    SELECT concat_syscalls_to_metadata_dag(%s, %s)
                 """, (meta.slave_addr, array_for_db))
