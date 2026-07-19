#!/usr/bin/env python3

from datetime import datetime
from typing import Callable

from python.executor.builtins import ConcurrencyError
from python.types import ReferenceTo
from python.utils.conn_factory import Conn


def update_timestamp(item_addr: ReferenceTo, conn: Conn) -> None:
    """
    Updates the edited_at timestamp in vector_ops to NOW();
    """
    conn.execute("""
    UPDATE vector_ops
        SET updated_at = NOW();
    WHERE addr = %s;
                 """, (item_addr, ))


def occ_check(prev_timestamp: datetime, item_addr: ReferenceTo, conn: Conn, new_content_fetch_function: Callable[[], str]) -> None:
    """
    Performs the occ check. Raises ConcurrencyError(new_content_fetch_function()) if check failed. 
    """

    last_edited = conn.execute_fetchval("""
    SELECT updated_at FROM vector_ops WHERE addr = %s
                                        """, (item_addr,))
    if last_edited > prev_timestamp:
        raise ConcurrencyError(new_content_fetch_function())
