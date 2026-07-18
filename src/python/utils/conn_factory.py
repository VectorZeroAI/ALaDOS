#!/usr/bin/env python3

import psycopg
from typing import Iterator, LiteralString, Sequence, cast, Any, overload, Literal
import types
from psycopg.rows import TupleRow
from psycopg.types import composite
from psycopg.sql import SQL

class NoValue(RuntimeError):
    def __init__(self, *error: str):
        self.error = error
    def __str__(self) -> str:
        return str(self.error)

class Conn(psycopg.Connection):
    def execute_fetchval(self, querry: SQL|LiteralString, params: Sequence = []) -> Any: ...

    @overload
    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[True]) -> list[TupleRow]: ...

    @overload
    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[False]) -> None: ...

    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: bool = False) -> None|Any: ...

def _execute_fetchval(self: Conn, querry: SQL|LiteralString, params: Sequence = []) -> Any:
    tuple_row = self.execute(querry, params).fetchone()
    if tuple_row:
        try:
            return tuple_row[0]
        except KeyError as e:
            try:
                return list(tuple_row)[0]
            except Exception as e2:
                raise NoValue(f"returned tuple row doesnt have any items, returned shape {tuple_row}, tuple_row[0] failed with KeyError {e}.",f"REcovery failed due to {e2}, idea of recovery was to extract the through list() on the result and then [0].")
    else:
        raise RuntimeError("Database returned no answer to the querry!")


def _execute_many(self: Conn, querry, params_seq, returning = False) -> list[TupleRow]|None:
    with self.cursor() as cur:
        cur.executemany(querry, params_seq, returning=returning)
        if returning:
            rows = []
            for subcur in cur.results():
                rows.extend(subcur.fetchall())
            return rows
        else:
            return None



def conn_factory() -> Conn:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.
    """
    conn = conn_factory_raw()

    conn = register_all_the_composite_types(conn)

    conn = cast(Conn, conn)

    conn.execute_fetchval = types.MethodType(_execute_fetchval, conn)
    conn.executemany = types.MethodType(_execute_many, conn)

    return cast(Conn, conn)


def register_all_the_composite_types(conn: psycopg.Connection) -> psycopg.Connection:

    rmt_node_info = composite.CompositeInfo.fetch(conn, "rmt_node")
    assert rmt_node_info is not None
    RmtNodeClass = composite.register_composite(rmt_node_info)
    conn.RmtNodeClass = RmtNodeClass # pyright: ignore
    return conn

def conn_factory_raw() -> psycopg.Connection:
    conn = psycopg.connect(
        host='/data/data/com.termux/files/usr/tmp',
        dbname='alados'
    )
    conn.autocommit = True
    return conn
