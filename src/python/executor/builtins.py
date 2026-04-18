#!/usr/bin/env python3

import os

import psycopg
from .execute_tool import register_tool
from ..utils.conn_factory import conn_factory
import subprocess
from ..types import ValidTables
import json
from .embedder import embedder

# EXAMPLE: 
@register_tool("print.to_console")
def print_to_console(input_str: str, _master_id: int) -> None:
    print(input_str)

@register_tool("K.create")
def k_create(content: str, description: str, name: str|None = None, _master_id: int = 99) -> None:
    """ 
    Creates a knowledge item.
    The description is a short definition of the items contents for semantic similarity search.
    Content is the actual content, and name is name wich can be used a access the item.
    Name of a knowledge item CANNOT be used in goal.add_slave required_results_names.
    """
    conn = conn_factory()

    addr = conn.execute("SELECT new_addr();").fetchone()[0] # pyright: ignore
    conn.execute("""
    INSERT INTO knowledge (addr, content, description) VALUES (%s, %s, %s);
                 """, (addr, content, description))
    if name is not None:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr,name))
    conn.close()
    return None

@register_tool("K.read")
def k_read(addr: int|None = None, name: str|None = None, _master_id: int = 99) -> str:
    """ Reads a knowledge item by address or by name. One of those must be provided. """
    conn = conn_factory()
    if addr is not None:
        result = conn.execute("""
        SELECT content FROM knowledge WHERE addr = %s
                     """, (addr,)).fetchone()[0]
    elif name is not None:
        result = conn.execute("""
        SELECT k.content FROM knowledge k JOIN names n ON k.addr = n.addr WHERE n.name = %s
                     """, (name,)).fetchone()[0]
    else:
        raise TypeError("ADDR OR NAME MUST BE PROVIDED")
    return result

@register_tool("tool.execute")
def execute_tool(addr: int|None, name: str|None, timeout: int = 10, kwargs: dict|None=None, _master_id: int = 99) -> str:
    """ 
    Executes a tool beyond buildins, from the database, by address or name.
    One of addr or name must not be None. 
    kwargs are the parameters you pass to the programm. They are json serialised, so do not try to pass in anything other then json.
    Timeout is the execution timeout, e.g. after how much time to kill the process and call it a failiure, in seconds.

    """
    conn = conn_factory()
    if name is not None:
        v_addr = conn.execute("""
        SELECT resolve_name(%s);
                              """, (name,)).fetchone()[0]
    else:
        v_addr = addr

    body = conn.execute("""
    SELECT body FROM executables WHERE addr = %s;
                        """, (v_addr,)).fetchone()[0]
    env = os.environ.copy()
    env["KWARGS"] = json.dumps(kwargs)
    
    result = subprocess.run(["python3"],
                                 input=body,
                                 capture_output=True,
                                 text=True,
                                 timeout=timeout,
                                 env=env
                                 )

    return result.stdout # TODO : add error handling and stderr capturing on error.

@register_tool("context.add")
def context_add_by_addr(addr: int|None, name: str|None, _master_id: int) -> None:
    """ Adds an item to the context by addr or by Name. Addr or Name must be provided. Items of any type may be added via this function. """
    conn = conn_factory()
    if addr is None:
        addr = conn.execute("""
        SELECT resolve_name(%s);
                            """, (name,)).fetchone()[0]
    
    conn.execute("""
    INSERT INTO master_load(master_addr, item_addr) VALUES (%s, %s)
                 """, (_master_id, addr))
    return None

@register_tool("goal.add_slave")
def add_slave(instruction: str,
              required_results_names: list[str]|None=None,
              required_results_addrs: list[int]|None=None,
              goal_name: str|None=None,
              result_name: str|None=None,
              _master_id: int = 99) -> None:
    """
    Adds a step to the task. The steps are executed asyncronosly, the moment all of their requirements are resolved. 
    A step may require anouther steps result, by adding the required results name or address. 
    A step gets the results it requires when it is executed.
    Each step is an separate instruction, to be executed, to produce a result, and to pass the result to the next step.
    required_results_names and required_results_addrs are for RESULTS OF SLAVES, not RESULTS OF TOOL CALLS.
    You can assume top down execution of the tool calls you wrote, but asynchronous execution of the slave goals themself.

    Example:
        {
            "tool": "goal.add_slave",
            "args": {
                "instruction": "print 'test_slave_executed_success' to console",
                "result_name": "printer_task"
            }
        },
        {
            "tool": "goal.add_slave",
            "args": {
                "required_results_names": ["printer_task"],
                "instruction": "Print 'second_slave_executed_successfully'"
            }
        }
    """
    conn = conn_factory()
    if required_results_addrs is None:
        required_results_addrs = []

    if required_results_names is not None:
        for i in required_results_names:
            required_results_addrs.append(conn.execute("""
            SELECT resolve_name(%s);
                  """, (i,)).fetchone()[0])

    conn.execute("""
    SELECT new_slave(%s, %s, %s, %s, %s, %s);
        """, 
    (_master_id, instruction, goal_name, required_results_addrs, None, result_name))
    return None

@register_tool("goal.add_planner_slave")
def add_replanner_slave(_master_id: int) -> None:
    """ Adds a planner step, that adds further steps, ensuring the whole plan of the task is created incrementally. """
    conn = conn_factory()
    special_context = []
    fetch = conn.execute("""
    SELECT instruction FROM masters WHERE addr = %s;
                         """, (_master_id,)).fetchone()
    assert fetch is not None
    special_context.extend(fetch)

    fetch = conn.execute("""
    SELECT s.instruction, r.content_str FROM masters m JOIN slaves s ON s.master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_master_id,)).fetchall()
    special_context.extend(fetch)

    special_context_str = f"Task instruction: {special_context.pop(0)}"

    tmp = []
    for i in special_context: # NOTE : the first element is removed in special_context.pop(0) call.
        tmp.append("\n")
        tmp.append("previous step: [")
        tmp.append(f" instruction: {i[0]}")
        tmp.append(f" result: {i[1]}")
        tmp.append("]")
    special_context_str = special_context_str + "".join(tmp)

    masters_result_so_far_str = conn.execute("SELECT master_result FROM master_context WHERE addr = %s", (_master_id,)).fetchone()
    masters_result_so_far_str = f"Masters result so far: {masters_result_so_far_str[0] if masters_result_so_far_str is not None else "No master result so far."}"

    fetch = conn.execute("""
    SELECT s.result_addr FROM masters m JOIN slaves s ON master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_master_id,)).fetchall()

    prompt = """
    Your task is to provide additional steps for the following task, given the previous steps and their results.
    You must only provide the direct next steps, after wich you must add the planner step via its dedicated tool, unless the task would be done.
    If the task is done, add a slave that gets all the finalised results and writes them as "result.master_result".
    For adding new steps, use the goal.add_slave tool. For adding a planner tool, use the goal.add_planner_slave tool.
    """ + special_context_str + masters_result_so_far_str

    conn.execute("SELECT new_slave(%s, %s, NULL, %s);", (_master_id, prompt, [r[0] for r in fetch]))

@register_tool("result.add_master_result")
def master_result_add(text: str, _master_id: int = 9) -> None:
    """
    This funtion adds a result for the whole master, e.g. the task that consists of many slaves.
    Doesnt actually terminate the master, and can be used multiple times.
    """
    conn = conn_factory()
    conn.execute("""
    UPDATE master_context SET master_result = master_result || %s WHERE addr = %s
                 """, (text, _master_id))

@register_tool("context.window.semantic_land")
def context_window_lands(querry: text, _master_id: int = 9) -> None:
    """
    Lands a viewing window, or a context window, these are the same thing, based on a semantic querry. 
    A viewing window is a dynamic automatic context window capable of providing you with relevant and highly controllable context
    of relevant knowledge and tools to be executed via tool.execute .
    Very important generally. 
    """
    conn = conn_factory()

    emb = embedder.encode_query(querry)

    conn.execute("""
    SELECT s_land(%s, %s)
                 """, (emb, _master_id))

@register_tool("context.window.land_by_addr")
def context_window_land(addr: int, _master_id: int) -> None:
    """
    Lands a viewing window, or a context window, these are the same thing, onto an addr.
    """
    conn = conn_factory()

    try:
        addr_type = conn.execute("""
        SELECT type FROM addrs_tables WHERE addr = %s;
                                 """, (addr,)).fetchone()[0]
    except TypeError as e:
        raise psycopg.DataError(f"Couldnt resolve addr {addr} to type, due to the following error: {e}, as result of fetch is not subscriptable.")
    if addr_type == "knowledge":
        conn.execute("""
        UPDATE master_context SET window_anchor_knowledge = %s, window_size_r = 12, window_size_l = 12;
                     """, (addr,))

    elif addr_type == "executables":
        conn.execute("""
        UPDATE master_context SET window_anchor_exe = %s, window_size_r = 12, window_size_l = 12;
                     """, (addr,))
