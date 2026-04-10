#!/usr/bin/env python3

from python.types import ReferenceTo
from .types import SlaveObj, WindowData, LoadsData
from ...utils.conn_factory import conn_factory
from pydantic import TypeAdapter, ValidationError
from typing import Any
import psycopg
from ...executor.execute_tool import HEADERS_REGISTRY


TOOL_HEADERS = "\n\n".join(HEADERS_REGISTRY.values())

def resolve_context(slave_obj: SlaveObj):
    conn = conn_factory()

    window_data: Any = conn.execute("""
    SELECT window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l FROM master_context WHERE addr = %s;
                 """, (slave_obj['master_addr'],)).fetchone()

    if window_data is not None:
        window_data_python: WindowData = {
                "master_addr": slave_obj['master_addr'],
                "window_position": {
                    "ref_addr": window_data[0] if window_data[0] is not None else window_data[1],
                    "ref_table": "executables" if window_data[0] is not None else "knowledge"
                    },
                "window_size_l": window_data[3],
                "window_size_r": window_data[2]
                }
        window_data_validator = TypeAdapter(WindowData)
        try:
            window_data_valid = window_data_validator.validate_python(window_data_python)
        except ValidationError as e:
            print(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")
            raise RuntimeError(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")

        window_context = resolve_window(window_data_valid)
    else:
        window_context = "VIEW WINDOW DOES NOT YET EXIST."

    load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = %s;
                             """, (slave_obj['master_addr'],)).fetchall()

    if load_data is not None:

        loads_data_python: LoadsData = {
                "items_addrs": [addr[0] for addr in load_data],
                "master_addr": slave_obj["master_addr"]
            }
        loads_data_validator = TypeAdapter(LoadsData)
        try:
            loads_data_valid = loads_data_validator.validate_python(loads_data_python)
        except ValidationError as e:
            print(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")
            raise RuntimeError(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")

        load_context = resolve_loads(loads_data_valid)
    else:
        load_context = "NO ITEMS LOADED YET"

    results_context = resolve_req_results(slave_obj, conn)

    return "\n\n\n".join([window_context, load_context, results_context, TOOL_HEADERS])

def resolve_req_results(slave_obj: SlaveObj, conn: psycopg.Connection):
    """ resolves the required results of a slave to their content_strings concated all into a single string blob. """
    req_results_addrs = conn.execute("""
    SELECT req_addr FROM slave_req WHERE slave_addr = %s;
                                     """, (slave_obj["addr"], )).fetchall()
    req_results_content = []
    for i in req_results_addrs:
        content = conn.execute("""
        SELECT content_str FROM results WHERE addr = %s;
                               """, (*i,)).fetchone()[0]
        req_results_content.append(content)

    results_str = "\n\n".join(req_results_content)
    return results_str
    

def _resolve_knowledge_item(addr: int, conn: psycopg.Connection) -> str:
    """ The function for resolving knowledge item to a clean AI friendly string """
    item = conn.execute("""
        SELECT names.name, knowledge.content
            FROM knowledge JOIN names ON names.addr = %s WHERE knowledge.addr = %s;

                 """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = ""
    result = "@".join((item[0], f"{addr}", "knowledge"))
    result = "\n".join(("", result, item[1], "", "", ""))
    return result

def _executables_item_resolve(addr: int, conn: psycopg.Connection) -> str:
    item = conn.execute("""
        SELECT names.name, executables.header, executables.body
            FROM executables JOIN names ON names.addr = %s WHERE executables.addr = %s;
                        """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"
    result = ""
    result = "@".join((item[0], f"{addr}", "executable"))
    result = "\n".join(("", "", result, f"header: {item[1]}", f"body: {item[2]}", "", "", ""))

    return result

def resolve_loads(loads_data: LoadsData) -> str:
    """ Resolves loads raw data to context string """
    conn = conn_factory() # FIXME: make ruff ignore E703 in ruff config.

    result_str: list[str] = []
    for addr in loads_data['items_addrs']:
        table = conn.execute("""
        SELECT type FROM addrs_tables WHERE addr = %s 
                     """, (addr,)).fetchone()[0]

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

def _result_item_resolve(addr: int, conn: psycopg.Connection):
    item = conn.execute("""
    SELECT s.result_name, 
        r.content_str,
        r.ready 
        FROM results r JOIN slaves s ON s.result_addr = %s WHERE r.addr = %s;
                        """, (addr, addr)).fetchone()
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = "@".join((item[0], f"{addr}"))
    result = "\n".join(("", "", result, f"content: {item[1]}", f"ready?: {item[2]}"))
    return result

def _slaves_item_resolve(addr: int, conn: psycopg.Connection) -> str:
    fetch = conn.execute("""
        SELECT names.name,
            slaves.master_addr,
            slaves.instruction,
            slaves.result_addr,
            slaves.result_name
            FROM slaves JOIN names ON names.addr = %s WHERE slaves.addr = %s;
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

def _masters_item_resolve(addr: int, conn: psycopg.Connection) -> str:
    slaves_fetch = conn.execute("""
        SELECT instruction, result_addr, result_name FROM slaves WHERE master_addr = %s;
                        """, (addr,)).fetchall()
    name = conn.execute("""
        SELECT name FROM names WHERE addr = %s;
                        """, (addr,)).fetchone()
    if name is None and slaves_fetch is None:
        return f"DOES NOT EXIST@{addr}"

    if name is None:
        name = ("None",)

    if slaves_fetch is None:
        slaves_fetch = ("No slaves", "no slaves", "no slaves")

    slave_str_list: list[str] = []
    result_str = "@".join((*name, f"{addr}", "master_goal"))
    result_str = "\n".join(("", "", result_str))
    for i in slaves_fetch:
        slave_str_list.append("slave: {")
        slave_str_list.append(f"instruction: {i[0]}")
        slave_str_list.append(f"result_addr: {i[1]}")
        slave_str_list.append(f"result_name: {i[2]}")
        slave_str_list.append("}")

    result_str = "\n".join([result_str, *slave_str_list])
    return result_str

def _logs_item_resolve(addr: int, conn: psycopg.Connection) -> str:
    item = conn.execute("""
        SELECT names.name, logs.created_at, logs.action, logs.created_by
            FROM logs JOIN names ON names.addr = %s WHERE logs.addr = %s;
                        """, (addr, addr)).fetchone()
    
    if item is None:
        return f"DOES NOT EXIST@{addr}"

    result = "@".join((item[0], f"{addr}", "log_item"))
    result = "\n".join(("", "", result, str(item[1]), item[2], item[3], "", "", ""))
    return result

def resolve_window(window_data: WindowData) -> str:
    """ This function resolves a window from raw window data from the DB. It resolves to a context string. """
    conn = conn_factory()
    anchor_pos = conn.execute(f"""
    SELECT position FROM {window_data["window_position"]["ref_table"]} WHERE addr = %s 
                 """, (window_data["window_position"]["ref_addr"], )).fetchone()

    if anchor_pos is None:
        return f"DOES NOT EXIST@{window_data["window_position"]}"

    anchor_pos = int(anchor_pos[0])
    most_l_pos = anchor_pos - window_data['window_size_l']
    most_r_pos = anchor_pos + window_data['window_size_r']

    context_fetch = conn.execute("""
    SELECT description, addr, position FROM knowledge WHERE position BETWEEN %s AND %s
    UNION ALL
    SELECT description, addr, position FROM executables WHERE position BETWEEN %s AND %s ORDER BY position;
                                 """, (most_l_pos, most_r_pos, most_l_pos, most_r_pos)).fetchall()

    descriptions, addrs, positions = zip(*context_fetch)

    names = []
    for a in addrs:
        names_fetch = conn.execute("""
        SELECT name FROM names WHERE addr = %s 
                                   """, (a,)).fetchone()
        names.append(*names_fetch)

    context_str = ""
    for d, a, p, n in zip(descriptions, addrs, positions, names):
        context_str = context_str + "@".join((n, f"pos: {p}", f"addr: {a}"))
        context_str = "\n".join((context_str, d, " ", " "))

    return context_str
