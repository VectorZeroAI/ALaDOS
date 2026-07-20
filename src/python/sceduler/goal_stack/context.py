#!/usr/bin/env python3
from typing import Any

from pydantic import TypeAdapter, ValidationError

from ...executor.execute_tool import HEADERS_REGISTRY
from ...utils.conn_factory import Conn
from .types import Anchor, LoadsData, SlaveObj, WindowData


def resolve_context(slave_obj: SlaveObj, conn: Conn):

    window_data: Any = conn.execute("""
    SELECT window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l FROM master_context WHERE addr = %s;
                 """, (slave_obj.master_addr,)).fetchone()
    if window_data is not None:
        
        if not (window_data[0] is None and window_data[1] is None):

            window_data_python = WindowData(
                slave_obj.master_addr,
                Anchor(
                    window_data[0] if window_data[0] is not None else window_data[1],
                    "executables" if window_data[0] is not None else "knowledge"
                ),
                window_data[3],
                window_data[2]
            )
            window_data_validator = TypeAdapter(WindowData)
            try:
                window_data_valid = window_data_validator.validate_python(window_data_python)
            except ValidationError as e:
                print(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")
                raise RuntimeError(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")

            window_context = resolve_window(window_data_valid, conn)
        else:
            window_context = "VIEW WINDOW DOES NOT YET EXIST"
    else:
        window_context = "VIEW WINDOW DOES NOT YET EXIST."

    load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = %s;
                             """, (slave_obj.master_addr,)).fetchall()

    if len(load_data) != 0:

        loads_data_python = LoadsData(
            [addr[0] for addr in load_data]
        )
        loads_data_validator = TypeAdapter(LoadsData)
        try:
            loads_data_valid = loads_data_validator.validate_python(loads_data_python)
        except ValidationError as e:
            print(f"context resolution failed, the context fetched from DB is: {loads_data_python}, but validator says: {e}")
            raise RuntimeError(f"context resolution failed, the context fetched from DB is: {loads_data_python}, but validator says: {e}")

        load_context = resolve_loads(loads_data_valid, conn)
    else:
        load_context = "NO ITEMS LOADED YET"

    results_context = resolve_req_results(slave_obj, conn)

    claimed_items = resolve_claimed_items(slave_obj, conn)

    TOOL_HEADERS = HEADERS_REGISTRY[slave_obj.scope]

    return "\n\n\n".join([f"Current viewing window is: [{window_context}]",
                          f"Currently loaded items are: [{load_context}]",
                          f"Previous steps results are: [{results_context}]",
                          f"Tool headers are: [{TOOL_HEADERS}]",
                          f"Your current type is '{slave_obj.scope}'. Other slave types will have other tools available."
                          f"Currently claimed items are: [{claimed_items}], please release those items when you no longer require them."
                          ])


def resolve_claimed_items(slave_obj: SlaveObj, conn: Conn) -> str:
    """
    Resolved the claimed items to remind the AI of them, so it doesnt forget it has them claimed.
    """

    addrs_fetch = conn.execute("""
    SELECT addr FROM ownership WHERE owner = %s;
                 """, (slave_obj.master_addr,)).fetchall()

    addrs = [a[0] for a in addrs_fetch]

    result = "\n".join([f"You currently hold exclusive ownership over item at address {a}. " for a in addrs])
    return result
    





def resolve_req_results(slave_obj: SlaveObj, conn: Conn):
    """ resolves the required results of a slave to their content_strings concated all into a single string blob. """
    req_results_addrs = conn.execute("""
    SELECT req_addr FROM slave_req WHERE slave_addr = %s;
                             """, (slave_obj.addr, )).fetchall()
    
    list_req_results_addrs = [addr[0] for addr in req_results_addrs]
    if len(list_req_results_addrs) < 1:
        return "NO REQUIRED RESULTS PRESENT"

    
    fetch = conn.execute("""
    SELECT r.content_str, s.instruction, r.metadata FROM results r LEFT JOIN slaves s ON s.result_addr = r.addr WHERE r.addr = ANY(%s)
                         """, (list_req_results_addrs,)).fetchall()
    req_results_str_list = []
    for i in fetch:
        if not i[1]:
            req_results_str_list.append(f"(information required by your instruction '{i[0]} ', with metadata: '{i[2]}')")
            continue
        req_results_str_list.append(f"(previous step instruction: '{i[1]}', result it produced: {i[0]})")

    req_results_str = "\n".join(req_results_str_list)

    return req_results_str
    

def _resolve_knowledge_item(addr: int, conn: Conn) -> str:
    """ The function for resolving knowledge item to a clean AI friendly string """
    item = conn.execute("""
        SELECT names.name, knowledge.content
            FROM knowledge LEFT JOIN names ON names.addr = %s WHERE knowledge.addr = %s;

                 """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = ""
    result = "@".join((item[0], f"{addr}", "knowledge"))
    result = "\n".join(("", result, item[1], "", "", ""))
    return result

def _executables_item_resolve(addr: int, conn: Conn) -> str:
    item = conn.execute("""
        SELECT names.name, executables.header, executables.body
            FROM executables LEFT JOIN names ON names.addr = %s WHERE executables.addr = %s;
                        """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"
    result = ""
    result = "@".join((item[0], f"{addr}", "executable"))
    result = "\n".join(("", "", result, f"header: {item[1]}", f"body: {item[2]}", "", "", ""))

    return result

def resolve_loads(loads_data: LoadsData, conn: Conn) -> str:
    """ Resolves loads raw data to context string """

    result_str: list[str] = []
    for addr in loads_data.items_addrs:
        table = conn.execute_fetchval("""
        SELECT type FROM addrs_tables WHERE addr = %s 
            """, (addr,)) # FIXME : Refactor the addrs_tables view into a materialised view, else performance will screw you!

        match table:
            case 'knowledge':
                result_str.append(_resolve_knowledge_item(addr, conn))
            case 'executables':
                result_str.append(_executables_item_resolve(addr, conn))
            case 'logs':
                result_str.append(_logs_item_resolve(addr, conn))
            case 'masters':
                result_str.append(_masters_item_resolve(addr, conn))

            case 'slaves':
                result_str.append(_slaves_item_resolve(addr, conn))
            case 'results':
                result_str.append(_result_item_resolve(addr, conn))
            case _:
                raise ValueError(f"Database returned a non existant or invalid table name. Returned {table}, but its not a valid table name. If it is, please add that tables case to the above handler.")

    return "".join(result_str)

def _result_item_resolve(addr: int, conn: Conn):
    item = conn.execute("""
    SELECT n.name, 
        r.content_str,
        r.ready 
    FROM results r
        LEFT JOIN names n ON n.addr = r.addr
    WHERE r.addr = %s;
                        """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = "@".join((item[0], f"{addr}"))
    result = "\n".join(("", "", result, f"content: {item[1]}", f"ready?: {item[2]}"))
    return result

def _slaves_item_resolve(addr: int, conn: Conn) -> str:
    fetch = conn.execute("""
        SELECT names.name,
            slaves.master_addr,
            slaves.instruction,
            slaves.result_addr,
            names2.name
        FROM slaves
            LEFT JOIN names ON names.addr = %s
            LEFT JOIN names names2 ON names2.addr = slaves.result_addr
        WHERE slaves.addr = %s;
                        """, (addr, addr)).fetchone()

    if fetch is None:
        return f"DOES NOT EXIST@{addr}"

    result = "@".join((fetch[0], f"{addr}", "slave_goal"))
    result = "\n".join(("", "", result,
                        f"master_addr: {fetch[1]}",
                        f"instruction: {fetch[2]}",
                        f"result_addr: {fetch[3]}",
                        f"result_name: {fetch[4]}",
                        "", "", ""))
    return result

def _masters_item_resolve(addr: int, conn: Conn) -> str:
    slaves_fetch = conn.execute("""
        SELECT s.instruction,
            s.result_addr,
            n.name
        FROM slaves s
            LEFT JOIN names n ON n.addr = s.result_addr
        WHERE master_addr = %s;
                        """, (addr,)).fetchall()
    name = conn.execute("""
        SELECT name FROM names WHERE addr = %s;
                        """, (addr,)).fetchone()
    if name is None and slaves_fetch is None:
        return f"DOES NOT EXIST@{addr}"

    if name is None:
        name = ("None",)

    slave_str_list: list[str] = []
    result_str = "@".join((*name, f"{addr}", "master_goal"))
    result_str = "\n".join(("", "", result_str))
    for i in slaves_fetch:
        slave_str_list.append("slave: {")
        slave_str_list.append(f"instruction: {i[0]}")
        slave_str_list.append(f"result_addr: {i[1]}")
        slave_str_list.append(f"result_name: {i[2]}")
        slave_str_list.append("}")

    if len(slaves_fetch) == 0:
        slave_str_list.append("No slaves present in the master goal")

    result_str = "\n".join([result_str, *slave_str_list])
    return result_str

def _logs_item_resolve(addr: int, conn: Conn) -> str:
    item = conn.execute("""
        SELECT names.name, logs.content, logs.created_at
            FROM logs LEFT JOIN names ON names.addr = logs.addr WHERE logs.addr = %s;
                        """, (addr,)).fetchone()
    
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = "@".join((item[0], f"{addr}", "log_item"))
    result = "\n".join(("", "", result, str(item[1]), item[2], "", "", ""))
    return result

def resolve_window(window_data: WindowData, conn: Conn) -> str:
    """ This function resolves a window from raw window data from the DB. It resolves to a context string. """
    anchor_pos = conn.execute("""
    SELECT position FROM vector_ops WHERE addr = %s 
                 """, (window_data.window_position.ref_addr, )).fetchone()

    if anchor_pos is None:
        return f"DOES NOT EXIST@{window_data.window_position}"

    anchor_pos = int(anchor_pos[0])

    context_fetch = conn.execute("""
    WITH ordered AS (
        SELECT description,
            addr,
            position,
            type,
            ROW_NUMBER() OVER (ORDER BY position) AS rn FROM vector_ops 
    ), anchor AS (
        SELECT rn FROM ordered WHERE addr = %s LIMIT 1
    )
    SELECT description, addr, position
    FROM ordered o, anchor a
    WHERE o.rn BETWEEN a.rn - %s AND a.rn + %s;
                             """, (window_data.window_position.ref_addr, window_data.window_size_l, window_data.window_size_r)).fetchall()

    descriptions, addrs, positions = zip(*context_fetch)

    names_fetch = conn.execute("""
    SELECT name, addr FROM names WHERE addr = ANY(%s)
                               """, (list(addrs),)).fetchall()
    assert names_fetch is not None

    addr_name_map = {}
    for nrow in names_fetch:
        addr_name_map[nrow[1]] = nrow[0]

    context_str = ""
    for d, a, p in zip(descriptions, addrs, positions):
        context_str = context_str + "@".join((addr_name_map.get(a, "Nameless"), f"pos: {p}", f"addr: {a}"))
        context_str = "\n".join((context_str, d, " ", " "))

    return context_str
