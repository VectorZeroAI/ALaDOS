#!/usr/bin/env python3

import psycopg
from typing import LiteralString, Sequence, cast, Any
import types
from psycopg.types import composite
from psycopg.sql import SQL

class Conn(psycopg.Connection):
    def execute_fetchval(self) -> Any: ...

def _execute_fetchval(self: Conn, querry: SQL|LiteralString, params: Sequence = []) -> Any:
    tuple_row = self.execute(querry, params).fetchone()
    if tuple_row:
        return tuple_row[0]
    else:
        raise RuntimeError("Database returned no answer to the querry!")

def conn_factory() -> Conn:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.
    """
    conn = psycopg.connect(
            host="127.0.0.1",
            port=5432,
            dbname="alados"
            )

    conn.autocommit = True

    conn = register_all_the_composite_types(conn)

    conn.execute_fetchval = types.MethodType(_execute_fetchval, conn) # pyright: ignore

    return cast(Conn, conn)


def register_all_the_composite_types(conn: psycopg.Connection) -> psycopg.Connection:

    rmt_node_info = composite.CompositeInfo.fetch(conn, "rmt_node")
    assert rmt_node_info is not None
    RmtNodeClass = composite.register_composite(rmt_node_info)
    conn.RmtNodeClass = RmtNodeClass # pyright: ignore
    return conn
