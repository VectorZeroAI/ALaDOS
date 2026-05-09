#!/usr/bin/env python3

import os
from typing import Sequence, TypeAlias, get_args

from numpy import ndarray
import psycopg
from .execute_tool import register_tool
import subprocess
import re
import json
from .embedder import embedder
from .types import _ExecToolMetaData, SlaveScope







@register_tool("tool.create", ['all', 'context'])
def create_tool(description: str, header: str, body: str, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """
    Creates a python tool, to be executed with tool.execute .
    Description is a short description used for searching and identifing the tool.
    header is detailed description of how to use the tool, including its signature.
    Body is the executed code itself. (Python only)
    Input parameters are accepted as key word arguments json object passed at key KWARGS into the env at execution time.
    Include estimated runtime, because execution longer then 10 seconds will time out without finishing unless timeout is specified to be longer.
    """
    conn = _meta['conn']
    addr = conn.execute("""
    SELECT new_addr();
                        """).fetchone()[0]
    conn.execute("""
    INSERT INTO executables(description, header, body, addr) VALUES(%s, %s, %s, %s);
                 """, (description, header, body, addr))
    if name is not None:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s);
                     """, (addr, name))

    _meta['_embedder_queue'].put(addr)

    return f"Created tool {name or description}@{addr}"



@register_tool("tool.edit", ['all', 'general', 'context'])
def edit_tool(name: str|None = None,
              addr: int|None = None,
              header_change: search_and_replace_block|None = None,
              body_change: search_and_replace_block|None = None,
              new_description: str|None = None,
              _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """
    Edit a tool.
    You must provide either header_change or body_change or new_description.
    You must provide either name or addr of the tool you want to edit.
    Header change or body change format is 'SEARCH AND REPLACE blocks'
    The format is the following:
    <SEARCH>
    def add(a, b):
    </SEARCH>
    <REPLACE>
    def add(a: int, b: int) -> int:
    </REPLACE>

    Empty search means append to the end.
    Only one search and replace per tool call allowed. Make multiple tool calls for multiple edits.
    """

    if header_change is None and body_change is None and new_description is None:
        raise TypeError("No change provided. Unable to apply nothing.")
    if name is None and addr is None:
        raise TypeError("No addr or name provided. Unable to identify what tool to edit.")

    conn = _meta['conn']

    if addr is None:
        try:
            addr = conn.execute("""
            SELECT resolve_name(%s);
                                """, (name,)).fetchone()[0]
        except Exception as e:
            raise Exception("Name most likely does not exist.") from e

    if new_description is not None:
        conn.execute("""
        UPDATE executables SET description = %s WHERE addr = %s;
                     """, (new_description, addr))
        _meta['_embedder_queue'].put(addr)

    if body_change is not None:
        old_body = conn.execute("""
        SELECT body FROM executables WHERE addr = %s;
                                """, (addr,)).fetchone()[0]
        assert isinstance(old_body, str)

        search, replacement = _sr_block_parser(body_change)

        new_body = old_body.replace(search, replacement)

        conn.execute("""
        UPDATE executables SET body = %s WHERE addr = %s;
                     """, (new_body, addr))
    if header_change is not None:
        old_header = conn.execute("""
        SELECT header FROM executables WHERE addr = %s;
                                  """, (addr,)).fetchone()[0]
        assert isinstance(old_header, str)

        search, replacement = _sr_block_parser(header_change)

        new_header = old_header.replace(search, replacement)

        conn.execute("""
        UPDATE executables SET header = %s WHERE addr = %s;
                     """, (new_header, addr))

    return f"Applied the edits to the tool {name}@{addr}"

@register_tool("context.add", ['all', 'general', 'context'])
def context_add_by_addr(addr: int|None, name: str|None, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """ Adds an item to the context by addr or by Name. Addr or Name must be provided. Items of any type may be added via this function. """
    conn = _meta['conn']
    if addr is None:
        addr = conn.execute("""
        SELECT resolve_name(%s);
                            """, (name,)).fetchone()[0]
    
    conn.execute("""
    INSERT INTO master_load(master_addr, item_addr) VALUES (%s, %s)
                 """, (_meta['master_id'], addr))
    return f"Added context {name if name is not None else "No name"}@{addr}."

@register_tool("goal.add_slave", ['all', 'general', 'task'])
def add_slave(instruction: str,
              slave_type: SlaveScope = 'general',
              required_results_names: list[str]|None=None,
              required_results_addrs: list[int]|None=None,
              slave_name: str|None=None,
              result_name: str|None=None,
              _meta: _ExecToolMetaData=None) -> ActionConfirmation:
    f"""
    Adds a step to the task. The steps are executed asyncronosly, the moment all of their requirements are resolved. 
    A step may require anouther steps result, by adding the required results name or address. 
    A step gets the results it requires when it is executed.
    Each step is an separate instruction, to be executed, to produce a result, and to pass the result to the next step.
    required_results_names and required_results_addrs are for RESULTS OF SLAVES, not RESULTS OF TOOL CALLS.
    You can assume top down execution of the tool calls you wrote, but asynchronous execution of the slave goals themself.
    slave_type is the type of the slave being added. The differenses are the tools that it sees. There is a baseline of what tools each one sees, and tools only specialists see.
    Currently allowed slave_types are: {get_args(SlaveScope)}.
    """
    conn = _meta['conn']
    if required_results_addrs is None:
        required_results_addrs = []

    if required_results_names is not None:
        for i in required_results_names:
            required_results_addrs.append(conn.execute("""
            SELECT resolve_name(%s);
                  """, (i,)).fetchone()[0])

    conn.execute("""
    SELECT new_slave(%s, %s, %s, %s, %s, %s, %s);
        """, 
    (_meta['master_id'], instruction, slave_name, required_results_addrs, None, result_name, slave_type))
    return "Added a new slave"

@register_tool("goal.add_planner_slave", ['all', 'task'])
def add_replanner_slave(_meta: _ExecToolMetaData) -> ActionConfirmation:
    """ Adds a planner step, that adds further steps, ensuring the whole plan of the task is created incrementally. """
    conn = _meta['conn']
    special_context = []
    fetch = conn.execute("""
    SELECT instruction FROM masters WHERE addr = %s;
                         """, (_meta['master_id'],)).fetchone()
    assert fetch is not None
    special_context.extend(fetch)

    fetch = conn.execute("""
    SELECT s.instruction, r.content_str FROM masters m JOIN slaves s ON s.master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_meta['master_id'],)).fetchall()
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

    masters_result_so_far_str = conn.execute("SELECT master_result FROM master_context WHERE addr = %s", (_meta['master_id'],)).fetchone()
    masters_result_so_far_str = f"Masters result so far: {masters_result_so_far_str[0] if masters_result_so_far_str is not None else "No master result so far."}"

    fetch = conn.execute("""
    SELECT s.result_addr FROM masters m JOIN slaves s ON master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_meta['master_id'],)).fetchall()

    prompt  =  """
    You task is to decide how to further proceed. For a given task,
    the given results and the master results,
    either formulate the next plan steps,
    or finalise the master result, if you already have enough information from the previous steps and their results,
    or do nothing, if the master result is already finalised enough. 
    DO NOT ADD SLAVES WITH THE SAME TASK REPETETIVELY!!!
    DO NOT TRY TO PLAN ALL STEPS AT ONCE.
    The task is complete if the master instruction is fully answered via the current master result. 
    """

    prompt = prompt + special_context_str + masters_result_so_far_str

    conn.execute("SELECT new_slave(%s, %s, NULL, %s);", (_meta['master_id'], prompt, [r[0] for r in fetch]))
    return "added a replanner slave"

@register_tool("result.add_master_result", ALL)
def master_result_add(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    This funtion writes a result for the whole master, e.g. the task that consists of many slaves.
    Newly written result is appended to the master result, it does not overwrite the result.
    """
    conn = _meta['conn']
    conn.execute("""
    UPDATE master_context SET master_result = master_result || %s WHERE addr = %s
                 """, (text, _meta['master_id']))
    return "Added a master result."

@register_tool("context.window.semantic_land", ['all', 'context'])
def context_window_lands(querry: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Lands a viewing window, or a context window, these are the same thing, based on a semantic querry. 
    A viewing window is a dynamic automatic context window capable of providing you with relevant and highly controllable context
    of relevant knowledge and tools to be executed via tool.execute .
    Very important generally. 
    """
    conn = _meta['conn']

    emb = embedder.encode_query(querry)

    if isinstance(emb, ndarray):
        emb = emb.tolist()

    conn.execute("""
    SELECT s_land(%s, %s::vector(768))
                 """, (_meta['master_id'], emb))
    return 'Semantically moved the viewing window anchor.'

@register_tool("context.window.land_by_addr", ['all', 'context'])
def context_window_land(addr: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Lands a viewing window, or a context window, these are the same thing, onto an addr.
    """
    conn = _meta['conn']

    try:
        addr_type = conn.execute("""
        SELECT type FROM addrs_tables WHERE addr = %s;
                                 """, (addr,)).fetchone()[0]
    except TypeError as e:
        raise psycopg.DataError(f"Couldnt resolve addr {addr} to type, due to the following error: {e}, as result of fetch is not subscriptable.")
    if addr_type == "knowledge":
        conn.execute("""
        UPDATE master_context SET
        window_anchor_knowledge = %s,
        window_anchor_exe = NULL,
        window_size_r = 12,
        window_size_l = 12
        WHERE addr = %s;
                     """, (addr, _meta['master_id']))

    elif addr_type == "executables":
        conn.execute("""
        UPDATE master_context SET
        window_anchor_exe = %s,
        window_anchor_knowledge = NULL,
        window_size_r = 12,
        window_size_l = 12
        WHERE addr = %s;
                     """, (addr, _meta['master_id']))
    else:
        raise psycopg.DataError(f"Invalid addr type gotten. Gotten {addr_type}, expected executables or knowledge.")
    return f"Moved context window center to {addr}"


@register_tool("context.window.change_size", ['all', 'context'])
def context_window_size_change(left: int = 0, right: int = 0, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ 
    The function for changing viewing windows size. 
    Negative number shrinks the size, positive number increases the size, possible in one or 2 directions.
    """
    conn = _meta['conn']
    
    conn.execute("""
    UPDATE master_context SET window_size_l = window_size_l + %s, window_size_r = window_size_r + %s WHERE addr = %s;
                 """, (left, right, _meta['master_id']))
    return "Changed context window size."

@register_tool("context.window.move_anchor", ['all', 'context'])
def move_window_anchor(amount: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Function to move the anchor of the viewing window.
    Moves to the left if amount if negative, to the right if amount is positive.
    """
    conn = _meta['conn']

    new_pos = conn.execute("""
    SELECT move_anchor(%s, %s);
                           """, (amount, _meta['master_id']))
    return "moved context window anchor"


@register_tool("result.write", ALL)
def result_write(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Writes plaintext passed in as the result to your current instruction, NOT to the master instruction.
    """
    return f"Result: {text}"


def report_paradoxal_information(items: Sequence[str|int], paradox: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Reports paradoxal items. Items are paradoxal if the information contained withhin them is mutually exclusive.
    paradox: the paradox in the information
    items: the list of items addresses or names that contain the paradoxal information.
    """

    conn = _meta['conn']

    conn.execute("""
    SELECT new_slave(%s, %s, '__paradox_recovery', %s);
                 """, (_meta['master_id'], _meta['']))
    
