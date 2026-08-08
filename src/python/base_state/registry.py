#!/usr/bin/env python3
"""
The registry file where the decorators and the patterns of registering items as base state are defined. 

The pattern of registering something is like this: 
    You import the class, e.g. type that you want to create an item of. 
    Its a dataclass.
    You instanciate it, and you decorate it with @register
"""

from typing import Callable, TypeAlias

from ..utils.conn_factory import conn_factory, Conn

from .types import Cronjob, Executable, Item, Knowledge, Masters, Results, Slaves


def register(item: Item) -> None:
    """
    The decorator that registers the Item
    """
    conn = conn_factory()
    with conn.transaction():
        match item:
            case Knowledge():
                register_knowledge(item, conn)
            case Executable():
                register_executable(item, conn)
            case Results():
                register_results(item, conn)

Registerer: TypeAlias = Callable[[Item, Conn], None]

REGISTERERS_REGISTRY = {}

def item_registerer(item_type: str):
    def wrapper(func):
        global REGISTERERS_REGISTRY
        REGISTERERS_REGISTRY[item_type] = func
        return func
    return wrapper


def register_item(item: Item, conn: Conn) -> None:
    REGISTERERS_REGISTRY[str(type(item))](item, conn)



@item_registerer(str(type(Cronjob)))
def register_cronjob(item: Cronjob, conn: Conn) -> None:
    if item.type == "once":
        conn.execute("""
        INSERT INTO cronjob_once()
                     """) 
    else:


    if item.name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s)
                     """, (item.addr, item.name))


@item_registerer(str(type(Slaves)))
def register_slaves(item: Slaves, conn: Conn) -> None:
    conn.execute("""
    DECLARE 
        t_addr BIGINT;
    BEGIN
        t_addr := %s;
        INSERT INTO slaves(addr, instruction, result_addr, scope) VALUES (t_addr, %s, %s, %s);
    END;
                 """, (item.addr, item.instruction, item.result_addr, item.scope))
    if item.deps:
        conn.executemany("""
        INSERT INTO slave_req(master_addr, req_addr) VALUES(%s, %s);
                         """,
                         [(item.addr, d) for d in item.deps],
                         returning=False
        )



@item_registerer(str(type(Masters)))
def register_master(item: Masters, conn: Conn) -> None:
    conn.execute("""
    DECLARE 
        t_addr BIGINT;
    BEGIN
        t_addr := %s;
        INSERT INTO masters(addr, instruction, result_addr) VALUES (t_addr, %s, %s);
    END;
                 """, (item.addr, item.instruction, item.result_addr))
    if item.deps:
        conn.executemany("""
        INSERT INTO master_req(master_addr, req_addr) VALUES(%s, %s);
                         """,
                         [(item.addr, d) for d in item.deps],
                         returning=False
        )

    if item.name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s)
                     """, (item.addr, item.name))



@item_registerer(str(type(Results)))
def register_result(item: Results, conn: Conn) -> None:
    conn.execute("""
    DECLARE 
        t_addr BIGINT;
    BEGIN
        t_addr := %s;
        INSERT INTO results(addr, content_str, metadata, ready) VALUES (t_addr, %s, %s);
    END;
                 """, (item.addr, item.content_str, item.metadata, item.ready))
    if item.name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s)
                     """, (item.addr, item.name))



@item_registerer(str(type(Executable)))
def register_executable(item: Executable, conn: Conn) -> None:
    conn.execute("""
    DECLARE 
        t_addr BIGINT;
    BEGIN
        t_addr := %s;
        INSERT INTO executables(addr, header, body) VALUES (t_addr, %s, %s);
        INSERT INTO vector_ops(addr, description) VALUES(t_addr, %s);
    END;
                 """, (item.addr, item.header, item.body, item.description))
    if item.name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s)
                     """, (item.addr, item.name))



@item_registerer(str(type(Knowledge)))
def register_knowledge(item: Knowledge, conn: Conn) -> None:
    conn.execute("""
    DECLARE
        t_addr BIGINT;
    BEGIN
        t_addr := %s;
        INSERT INTO knowledge(addr, content) VALUES(t_addr, %s);
        INSERT INTO vector_ops(addr, content) VALUES(t_addr, %s);
    END;
                 """, (item.addr, item.content, item.description))
    if item.name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s)
                     """, (item.addr, item.name))
