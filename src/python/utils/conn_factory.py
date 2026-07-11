#!/usr/bin/env python3

import psycopg
from typing import Iterator, LiteralString, Sequence, cast, Any, overload, Literal
import types
from psycopg.rows import TupleRow
from psycopg.types import composite
from psycopg.sql import SQL

class Conn(psycopg.Connection):
    def execute_fetchval(self, querry: SQL|LiteralString, params: Sequence = []) -> Any: ...
    def executemany(self, querry: SQL|LiteralString, params: Sequence[Sequence]) -> None: ...

def _execute_fetchval(self: Conn, querry: SQL|LiteralString, params: Sequence = []) -> Any:
    tuple_row = self.execute(querry, params).fetchone()
    if tuple_row:
        return tuple_row[0]
    else:
        raise RuntimeError("Database returned no answer to the querry!")

@overload
def _execute_many(self: Conn, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[True]) -> Iterator[psycopg.Cursor[TupleRow]]: ...

@overload
def _execute_many(self: Conn, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[False]) -> None: ...

def _execute_many(self, querry, params_seq, returning):
    with self.cursor() as cur:
        cur.executemany(querry, params_seq, returning=returning)
        if returning:
            return cur.results()
        else:
            return None



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
    conn.executemany = types.MethodType(_execute_many, conn) # pyright: ignore

    return cast(Conn, conn)


def register_all_the_composite_types(conn: Conn) -> psycopg.Connection:

    rmt_node_info = composite.CompositeInfo.fetch(conn, "rmt_node")
    assert rmt_node_info is not None
    RmtNodeClass = composite.register_composite(rmt_node_info)
    conn.RmtNodeClass = RmtNodeClass # pyright: ignore
    return conn
