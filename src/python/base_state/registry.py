#!/usr/bin/env python3
"""
The registry file where the decorators and the patterns of registering items as base state are defined. 

The pattern of registering something is like this: 
    You import the class, e.g. type that you want to create an item of. 
    Its a dataclass.
    You instanciate it, and you decorate it with @register
"""

from typing import Callable, Coroutine, TypeAlias
from psycopg.types.json import Jsonb
from functools import partial

from ..base_state.custom_consumer import consumer_outer

from ..utils.conn_factory import conn_factory, Conn
from ..rmt.main import create_from_serial

from .types import Cronjob, CustomConsumer, CustomListener, EventConsumers, Executable, Item, Knowledge, Masters, Results, Rmt, Slaves

REGISTERERS_REGISTRY = {}
SYSTEM_ADDRS_LIST: list[int] = []
ADDR_REGISTER: dict[int, Callable[[], None]] = {}
CUSTOM_CONSUMERS: list[Coroutine[None, None, None]] = []

def register(item: Item) -> Item:
    """
    The function that registers the item.
    
    For some reason you cant decorate an instance of a dataclass.
    """
    if isinstance(item, CustomConsumer):
        CUSTOM_CONSUMERS.append(
            consumer_outer(
                item.consumer_inner_callback,
                item.event_path
            )
        )
        return item

    if isinstance(item, CustomListener):
        CUSTOM_CONSUMERS.append(item.async_coro)
        return item


    conn = conn_factory()
    SYSTEM_ADDRS_LIST.append(item.addr)
    ADDR_REGISTER[item.addr] = partial(__register_item, item, conn)
    return item

Registerer: TypeAlias = Callable[[Item, Conn], None]


def __item_registerer(item_type: str):
    def wrapper(func):
        global REGISTERERS_REGISTRY
        REGISTERERS_REGISTRY[item_type] = func
        return func
    return wrapper


def __register_item(item: Item, conn: Conn) -> None:
    REGISTERERS_REGISTRY[str(type(item))](item, conn)

def insert_addr(addr: int, conn: Conn) -> None:
    if conn.execute_fetchval("""
        SELECT TRUE FROM addrs WHERE addr = %s;
                             """, (addr,)):
        return

    conn.execute("""
    INSERT INTO addrs(addr) VALUES(%s)
                 """, (addr,))

@__item_registerer("<class 'python.base_state.types.EventConsumers'>")
def register_event_consumer(item: EventConsumers, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO event_consumers(addr, event_path, action_type) VALUES(%s, %s, %s)
                     """, (item.addr, item.event_path, item.action_type))
        match item.action_type:
            case "call_rmt":
                conn.execute("""
                INSERT INTO event_call_rmt(addr, rmt_addr, args) VALUES(%s, %s::BIGINT, %s::JSONB)
                             """, (item.addr, item.field1, Jsonb(item.field2)))
            case "execute_slave":
                conn.execute("""
                INSERT INTO event_call_execute_slave(addr, instruction, scope) VALUES(%s, %s::TEXT, %s::slave_scope)
                             """, (item.addr, item.field1, item.field2))
            case "fill_result":
                conn.execute("""
                INSERT INTO event_call_fill_result(addr, result_addr, result_str) VALUES(%s, %s::BIGINT, %s::TEXT)
                             """, (item.addr, item.field1, item.field2))



@__item_registerer("<class 'python.base_state.types.Rmt'>")
def register_rmt(item: Rmt, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        auto_addr = create_from_serial(item.dsl, conn, item.name)
        conn.execute("""
        UPDATE addrs
            SET addr = %s
        WHERE addr = %s;
                     """, (item.addr, auto_addr))

        conn.execute("""
        INSERT INTO vector_ops(rmt_addr, description) VALUES (%s, %s);
                     """, (item.addr, item.description))



@__item_registerer("<class 'python.base_state.types.Cronjob'>")
def register_cronjob(item: Cronjob, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        if item.type == "once":
            conn.execute("""
            INSERT INTO cronjob_once(addr, name, args, start_after)
            VALUES (%s, %s, (EXTRACT(EPOCH FROM NOW()) + %s)::INT);
                         """, (item.addr, item.action_name, Jsonb(item.args), item.timelapse)) 
        else:
            conn.execute("""
            INSERT INTO cronjob_loop(addr, name, args, last_ran, execute_every)
            VALUES (%s, %s, %s, (EXTRACT( EPOCH FROM NOW())), %s);
                         """, (item.addr, item.action_name, Jsonb(item.args), item.timelapse)) 



@__item_registerer("<class 'python.base_state.types.Slaves'>")
def register_slaves(item: Slaves, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO slaves(addr, instruction, result_addr, scope) VALUES (%s, %s, %s, %s);
                     """, (item.addr, item.instruction, item.result_addr, item.scope))
        if item.deps:
            conn.executemany("""
            INSERT INTO slave_req(slave_addr, req_addr) VALUES(%s, %s);
                             """,
                             [(item.addr, d) for d in item.deps],
                             returning=False
            )



@__item_registerer("<class 'python.base_state.types.Masters'>")
def register_master(item: Masters, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO masters(addr, instruction, result_addr) VALUES (%s, %s, %s);
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



@__item_registerer("<class 'python.base_state.types.Results'>")
def register_result(item: Results, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO results(addr, content_str, metadata, ready) VALUES (%s, %s, %s, %s);
                     """, (item.addr, item.content_str, item.metadata, item.ready))
        if item.name:
            conn.execute("""
            INSERT INTO names(addr, name) VALUES(%s, %s)
                         """, (item.addr, item.name))



@__item_registerer("<class 'python.base_state.types.Executable'>")
def register_executable(item: Executable, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO executables(addr, header, body) VALUES (%s, %s::TEXT, %s::TEXT);
                     """, (item.addr, item.header, item.body))

        conn.execute("""
        INSERT INTO vector_ops(addr_exe, description) VALUES(%s, %s::TEXT);
                     """, (item.addr, item.description))
        if item.name:
            conn.execute("""
            INSERT INTO names(addr, name) VALUES(%s, %s)
                         """, (item.addr, item.name))



@__item_registerer("<class 'python.base_state.types.Knowledge'>")
def register_knowledge(item: Knowledge, conn: Conn) -> None:
    with conn.transaction():
        insert_addr(item.addr, conn)
        conn.execute("""
        INSERT INTO knowledge(addr, content) VALUES(%s, %s::TEXT);
                     """, (item.addr, item.content))

        conn.execute("""
        INSERT INTO vector_ops(addr_k, description) VALUES(%s, %s::TEXT);
                     """, (item.addr, item.description))
        if item.name:
            conn.execute("""
            INSERT INTO names(addr, name) VALUES(%s, %s)
                         """, (item.addr, item.name))


