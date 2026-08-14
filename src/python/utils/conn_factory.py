#!/usr/bin/env python3

import os
from typing import Any, Literal, LiteralString, Sequence, overload

import psycopg
from psycopg.rows import TupleRow
from psycopg.sql import SQL
from psycopg.types import composite


class NoValue(RuntimeError):
    def __init__(self, *error: str):
        self.error = error
    def __str__(self) -> str:
        return str(self.error)

class Conn(psycopg.Connection):
    def execute_fetchval(self, querry: SQL|LiteralString, params: Sequence = []) -> Any: 
        """
        Executes the querry and fetches a value, then returns the value. 
        !!! Raises a RuntimeError if no answer was returned !!!
        """
        tuple_row = self.execute(querry, params).fetchone()
        if tuple_row:
            try:
                return tuple_row[0]
            except KeyError as e:
                """
                This case is here for legacy reasons.
                There were bugs with different psycopg return methods around,
                and since then this thing is here.
                Dont touch, its uselles but just dont. 
                """
                try:
                    return list(tuple_row)[0]
                except Exception as e2:
                    raise NoValue(f"returned tuple row doesnt have any items, returned shape {tuple_row}, tuple_row[0] failed with KeyError {e}.",f"REcovery failed due to {e2}, idea of recovery was to extract the through list() on the result and then [0].")
        else:
            raise RuntimeError("Database returned no answer to the querry!")

    @overload
    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[True]) -> list[TupleRow]: ...

    @overload
    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: Literal[False]) -> None: ...

    def executemany(self, querry: SQL|LiteralString, params_seq: Sequence[Sequence], returning: bool = False) -> None|Any:
        with self.cursor() as cur:
            cur.executemany(querry, params_seq, returning=returning)
            if returning:
                rows = []
                for subcur in cur.results():
                    rows.extend(subcur.fetchall())
                return rows
            else:
                return None

def conn_factory(db_name: str|None = None) -> Conn:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.

    ALADOS_DB_NAME enviroment variable is read ofr the DB name,
    or DB name can be passed into the function.
    """
    conn = conn_factory_raw(db_name)

    conn = register_all_the_composite_types(conn)

    return conn


def register_all_the_composite_types(conn: Conn) -> Conn:

    rmt_node_info = composite.CompositeInfo.fetch(conn, "rmt_node")
    assert rmt_node_info is not None
    RmtNodeClass = composite.register_composite(rmt_node_info)
    conn.RmtNodeClass = RmtNodeClass # pyright: ignore
    return conn

def conn_factory_raw(db_name: str|None = None) -> Conn:

    db_name = db_name or os.environ.get("ALADOS_DB_NAME", "alados")

    conn = Conn.connect(
        host='/data/data/com.termux/files/usr/tmp',
        dbname=db_name,
    )
    conn.autocommit = True
    return conn


async def async_conn_factory_raw(db_name: str|None = None) -> psycopg.AsyncConnection:
    db_name = db_name or os.environ.get("ALADOS_DB_NAME", "alados")

    conn = await psycopg.AsyncConnection.connect(
        host='/data/data/com.termux/files/usr/tmp',
        db_name = db_name
    )
    conn.autocommit = True # TODO : Make async Conn

    return conn
