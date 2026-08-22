#!/usr/bin/env python3
"""
Top level helpers of ALaDOS python part.
"""

from pathlib import Path

import psycopg

from .utils.conn_factory import Conn


def ensure_schema_applied(conn: Conn) -> None:
    """ Ensures schema applied. """

    file = Path(__file__)
    sql_dir = file.parent.parent / "sql"

    for i in sorted(sql_dir.glob("*.sql")):
        try:
            conn.execute(i.read_text()) # pyright: ignore
            print(f"sql file {i.name} was successfully executed")
        except Exception as e:
            raise psycopg.DatabaseError(f"the setup of the db via the sql files failed. reason: {e}") from e
