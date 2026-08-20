#!/usr/bin/env python3

import os
from traceback import format_exception
from typing import (
    Any,
    Iterable,
    Literal,
    LiteralString,
    Sequence,
    overload,
)

import psycopg
from psycopg.rows import TupleRow
from psycopg.sql import SQL
from psycopg.types import composite

from ..types import ReferenceTo


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



    def coearse(self, item: Iterable[str]) -> tuple[list[int], list[str]]:
        """
        Tries to coerse the string into an int,
        which is defined edge case and is supposed to be handled that way. 

        "120947" -> 120947
        """

        coearsed: list[int] = []
        coearsed_worked: list[str] = []

        for n in item:
            try:
                coearsed.append(int(n))
                coearsed_worked.append(n)
            except ValueError:
                pass

        item_list = list(item)
        for i in range(len(item_list), 0, -1):
            if i in coearsed_worked:
                item_list.pop(i)

        return (coearsed, item_list)



    def batch_resolve_names(self, names: list[str]) -> list[int]:
        """
        Bypasses resolve_name, insdead goes directly to source. 
        """
        addrs_fetch: list[tuple[int]] = self.execute("""
        SELECT n.addr
        FROM unnest(%s::TEXT[]) WITH ORDINALITY AS q(name, pos)
            JOIN names n ON q.name = n.name
        ORDER BY q.pos;
                          """, (names,)).fetchall()

        addrs: list[int] = [a[0] for a in addrs_fetch]

        if len(names) > len(addrs):
            raise RuntimeError("Some names could not be resolved! Double check names correctness.")

        return addrs


    def resolve_to_addr(self, item: ReferenceTo|str) -> ReferenceTo:
        """
        Tries to resolve an items name if its name.
        Always returns address, raises RuntimeError if no address found.

        EDGE CASE:
            if its string coersed address, basically "183475203".
            
            This funcion DOES TRY TO COERSE, and the numeric only names are NOT ALLOWED.
            It is enforced at DB level that numbers only are not a valid name and are thus treated as address.
        """
        if isinstance(item, str):
            try:
                try:
                    return int(item)
                except ValueError:
                    pass
                return self.execute_fetchval("SELECT resolve_name(%s);", (item,))

            except Exception as e:
                log_json({
                    'type': 'util',
                    'subtype': 'name_resolver',
                    'status': 'fatal',
                    'error': str(e),
                    'traceback': str(format_exception(e))
                })
                raise RuntimeError(f"resolution failed due to {e}")

        return item

    def resolve_to_addrs(self, names_and_addrs: Iterable[ReferenceTo|str]) -> list[ReferenceTo]:
        """
        Resolved the the strings of a list into the numeric addressess.
        Raises RuntimeError if a name couldnt be resolved.
        """
        were: list[ReferenceTo] = []
        rest: list[str] = []

        for i in names_and_addrs:
            if isinstance(i, ReferenceTo):
                were.append(i)
            else:
                rest.append(i)

        try:
            coerse, rest = self.coearse(rest)
            resolved = self.batch_resolve_names(rest)
        except Exception as e:
            log_json({
                'type': 'util',
                'subtype': 'name_resolver',
                'status': 'fatal',
                'error': str(e),
                'traceback': str(format_exception(e))
            })
            raise RuntimeError(f"Resolution failed with error {e}, because resolve_name somehow let an None through, or something was wrong upstream")

        all: list[ReferenceTo] = []
        all.extend(were)
        all.extend(coerse)
        all.extend(resolved)

        return all
        


    def resolve_self(self, slave_addr: ReferenceTo, names_and_addrs: Sequence[str|ReferenceTo]) -> list[str|ReferenceTo]:
        """
        Resolved the "self" string in the input list to the slaves result address, slave address required.
        Later possibly will be expanded to include self_master as well.

        Resolves the slaves result addr automatically from the given addr.
        """
        result_addr = self.execute_fetchval("SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,))

        names_and_addrs = list(names_and_addrs)
        for i in range(len(names_and_addrs)):
            if names_and_addrs[i] == "self":
                names_and_addrs[i] = result_addr
        
        return names_and_addrs

    def resolve_id_to_name_and_addr(self, id: ReferenceTo|str) -> tuple[str, ReferenceTo]:
        """
        Resolves the id to the name and to the address.

        Defaults name to "NoName" if no Name found.
        """
        if isinstance(id, int):
            addr = id
            name = self.execute_fetchval("""
            SELECT COALESCE((SELECT name FROM names WHERE addr = %s LIMIT 1), 'NoName');
                                         """, (addr,))
        else:
            name = id
            addr = self.resolve_to_addr(name)
        return (name, addr)


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


from .logger import log_json # noqa F402
