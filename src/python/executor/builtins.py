#!/usr/bin/env python3
"""
The giant file where all the built in tools are located.  

!!! WARNING: TOOLS ARE BEING MIGRATED TO DB, SO THIS IS SOON TO BE SYSCALLS AND NOT TOOLS. !!!

A style I would want to enforce with rmts: 
    the rmt logic itself lives in the rmt/main file
    the context fill in logic, e.g. the builtins specific logic lives in here in builtins. 
    If you are analysing this file and you see a violation of this,
    please report it as a bug and cite this place here as proof that its a bug.

Syscalls have to return string because transportation layer depends on the return always being string. 
The lib side will have to translate to proper return type.
"""

import json
from dataclasses import asdict
from functools import partial
from typing import Any, Literal, Sequence, TypeAlias, get_args

import httpx
import psycopg
from numpy import ndarray
from psycopg.types.json import Jsonb

from ..events.functions import (
    create_result_via_event,
    register_reaction_execute_slave,
    register_reaction_rmt,
)
from ..rmt.main import (
    activate_as_master,
    change_scope,
    create_from_master,
    create_from_range,
    create_from_serial,
    delete_node,
    edit_instruction,
    insert_node,
    serialize,
)
from ..utils.conn_factory import NoValue
from ..utils.logger import log_json
from ..utils.name_resolver import resolve_self, resolve_to_addr, resolve_to_addrs
from ..utils.occ_functions import occ_check, update_timestamp
from ..utils.sr_edit import SearchAndReplaceBlock, _sr_block_parser
from .comms import httpsystem
from .comms.searxng import SearxngSearcher
from .cronjobs.parser import insert_cronjob
from .cronjobs.types import Cronjob, CronjobActions
from .embedder import embedder
from .exceptions import ParadoxDetected
from .execute_tool import register_tool, tools_manager
from .types import ReferenceTo, SlaveScope, _ExecToolMetaData

Addr: TypeAlias = ReferenceTo
Name: TypeAlias = str
ActionConfirmation: TypeAlias = str

ALL = get_args(SlaveScope)

searcher_obj = SearxngSearcher()

# def k_create(content: str, description: str, _meta: _ExecToolMetaData, name: str|None = None) -> ActionConfirmation:
# @register_tool("K.create", ['general', 'context'])
@register_tool("k_create", ['general', 'context'])
def k_create(content: str, description: str, _meta: _ExecToolMetaData, name: str|None = None) -> str:
    """ 
    Creates a knowledge item.
    The description is a short definition of the items contents for semantic similarity search.
    Content is the actual content, and name is name which can be used a access the item.
    Name of a knowledge item CANNOT be used in goal.add_slave required_results_names.
    """
    conn = _meta.conn

    addr = conn.execute_fetchval("SELECT new_addr();")

    conn.execute("""
    INSERT INTO knowledge (addr, content) VALUES (%s, %s);
                 """, (addr, content))
    conn.execute("""
    INSERT INTO vector_ops (addr_k, description) VALUES (%s, %s);
                 """, (addr, description))

    if name:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr, name))

    _meta._embedder_queue.put(addr)

    return str(addr)
    # return f"knowledge entry {name if name is not None else "No name"}@{addr} was created."

# def k_edit(_meta: _ExecToolMetaData,
#            id: Addr|str,
#            description_change: SearchAndReplaceBlock|None = None,
#            content_change: SearchAndReplaceBlock|None = None,
#            ) -> ActionConfirmation:

# @register_tool("K.edit", ['general', 'context'])
@register_tool("k_edit", ['general', 'context'])
def k_edit(_meta: _ExecToolMetaData,
           id: Addr|str,
           description_change: SearchAndReplaceBlock|None = None,
           content_change: SearchAndReplaceBlock|None = None,
           ) -> str:
    """
    Edits a knowledge entry. 
    Either addr or name must be provided
    change is in the same format as tool.edits change format.
    """
    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    def get_new_content() -> str:
        k_data = conn.execute("""
        SELECT k.content, v.description FROM knowledge k JOIN vector_ops v ON k.addr = v.addr WHERE k.addr = %s;
                                                 """, (addr,)).fetchone()
        assert k_data is not None

        return f"<item_content>{k_data[0]}</item_content><item_description>{k_data[1]}</item_description>"

    occ_check(_meta.occ_last_change, addr, conn, get_new_content)

    if content_change is not None:
        old_k = conn.execute_fetchval("""
        SELECT content FROM knowledge WHERE addr = %s;
                             """, (addr,))
        search, replace = _sr_block_parser(content_change)
        new_k = old_k.replace(search, replace)
        conn.execute("""
        UPDATE knowledge SET content = %s WHERE addr = %s;
                     """, (new_k, addr))

    if description_change is not None:
        old_d = conn.execute_fetchval("""
        SELECT description FROM vector_ops WHERE addr = %s;
                             """, (addr,))
        search, replace = _sr_block_parser(description_change)
        new_d = old_d.replace(search, replace)
        conn.execute("""
        UPDATE vector_ops SET description = %s WHERE addr = %s;
                     """, (new_d, addr))

        _meta._embedder_queue.put(addr)

    update_timestamp(addr, conn)

    return ""
    #return f"Edited the knowledge item {id if isinstance(id, str) else "Nameless"}@{addr}"





#def k_read(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:
#@register_tool("K.read", ['general', 'context'])
@register_tool("k_read", ['general', 'context'])
def k_read(_meta: _ExecToolMetaData, id: Addr|str) -> str:
    """ Resolve knowledge item by ID. """
    conn = _meta.conn
    addr = resolve_to_addr(id, conn)

    result = conn.execute_fetchval("""
    SELECT content FROM knowledge WHERE addr = %s
                 """, (addr,))

    return result
    #return f"Knowledge entry {id if isinstance(id, str) else "no name"}@{addr}, contents: {result}."


# def execute_tool_builtin_func(_meta: _ExecToolMetaData, id: Addr|str, timeout: int = 10, kwargs: dict|None=None) -> ActionConfirmation:
#@register_tool("tool.execute", ['general'])
@register_tool("tool_execute", ['general']) # TODO : Rename into something like "Execute" cause it executes executables, and last part can be left out.
def execute_tool_builtin_func(_meta: _ExecToolMetaData, id: Addr|str, timeout: int = 10, kwargs: dict|None=None) -> str:
    """ 
    Executes a tool by id.
    """

    conn = _meta.conn


    if isinstance(id, str):
        name = id
    else:
        name = conn.execute_fetchval("""
        SELECT name FROM names WHERE addr = %s;
                                     """, (id,))

    if kwargs is None:
        kwargs = {}
    kwargs["timeout"] = timeout

    return tools_manager[name](kwargs, _meta)


# def create_tool(description: str, header: str, body: str, _meta: _ExecToolMetaData, name: str|None = None) -> ActionConfirmation:
# @register_tool("tool.create", ['context'])
@register_tool("tool_create", ['context'])
def create_tool(description: str, header: str, body: str, _meta: _ExecToolMetaData, name: str|None = None) -> str:
    """
    Creates a python tool, to be executed with tool.execute .
    Description is a short description used for searching and identifing the tool.
    header is detailed description of how to use the tool, including its signature.
    Body is the executed code itself. (Python only)
    Input parameters are accepted as key word arguments json object passed at key KWARGS into the env at execution time.
    Include estimated runtime, because execution longer then 10 seconds will time out without finishing unless timeout is specified to be longer.
    """
    conn = _meta.conn
    addr = conn.execute_fetchval("""
        SELECT new_addr();
                        """)
    conn.execute("""
        INSERT INTO executables(header, body, addr) VALUES (%s, %s, %s);
                 """, (header, body, addr,))
    conn.execute("""
        INSERT INTO vector_ops(addr_exe, description) VALUES (%s, %s)
                 """, (addr, description))
    if name is not None:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s);
                     """, (addr, name))

    _meta._embedder_queue.put(addr)

    return str(addr)
    #return f"Created tool {name or description}@{addr}"


# def edit_tool(_meta: _ExecToolMetaData,
#               id: str|Addr,
#               header_change: SearchAndReplaceBlock|None = None,
#               body_change: SearchAndReplaceBlock|None = None,
#               new_description: str|None = None,
#               ) -> ActionConfirmation:

# @register_tool("tool.edit", ['general', 'context'])
@register_tool("tool_edit", ['general', 'context'])
def edit_tool(_meta: _ExecToolMetaData,
              id: str|Addr,
              header_change: SearchAndReplaceBlock|None = None,
              body_change: SearchAndReplaceBlock|None = None,
              new_description: str|None = None,
              ) -> str:
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

    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    def get_new_content() -> str:
        executable_data = conn.execute("""
        SELECT e.body, e.header, v.description FROM executables e JOIN vector_ops v ON e.addr = v.addr WHERE e.addr = %s;
                                          """, (addr,)).fetchone()
        assert executable_data is not None
        return f"<body>{executable_data[0]}</body><header>{executable_data[1]}</header><description>{executable_data[2]}</description>"

    occ_check(_meta.occ_last_change, addr, conn, get_new_content)
        
    if new_description is not None:
        conn.execute("""
        UPDATE vector_ops SET description = %s WHERE addr = %s;
                     """, (new_description, addr))
        _meta._embedder_queue.put(addr)

    if body_change is not None:
        old_body = conn.execute_fetchval("""
        SELECT body FROM executables WHERE addr = %s;
                                """, (addr,))
        assert isinstance(old_body, str)

        search, replacement = _sr_block_parser(body_change)

        new_body = old_body.replace(search, replacement)

        conn.execute("""
        UPDATE executables SET body = %s WHERE addr = %s;
                     """, (new_body, addr))
    if header_change is not None:
        old_header = conn.execute_fetchval("""
        SELECT header FROM executables WHERE addr = %s;
                                  """, (addr,))
        assert isinstance(old_header, str)

        search, replacement = _sr_block_parser(header_change)

        new_header = old_header.replace(search, replacement)

        conn.execute("""
        UPDATE executables SET header = %s WHERE addr = %s;
                     """, (new_header, addr))

    update_timestamp(addr, conn)

    return ""
    #return f"Applied the edits to the tool {id if isinstance(id, str) else 'No_Name'}@{addr}"




# def context_add(id: Addr|str, _meta: _ExecToolMetaData) -> ActionConfirmation:
# @register_tool("context.add", ['general', 'context'])
@register_tool("context_add", ['general', 'context'])
def context_add(id: Addr|str, _meta: _ExecToolMetaData) -> str:
    """ Adds an item to the context by addr or by Name. Addr or Name must be provided. Items of any type may be added via this function. """
    conn = _meta.conn
    
    addr = resolve_to_addr(id, conn)
    
    conn.execute("""
    INSERT INTO master_load(master_addr, item_addr) VALUES (%s, %s)
                 """, (_meta.master_id, addr))

    return ""
    #return f"Added context {id if isinstance(id, str) else "No name"}@{addr}."
    # TODO: Try to find a name and insert the name if found.


# def add_slave(instruction: str,
#               _meta: _ExecToolMetaData,
#               slave_type: SlaveScope = 'general',
#               required_results_ids: list[str|Addr] = [],
#               slave_name: str|None=None,
#               result_name: str|None=None
#               ) -> ActionConfirmation:


# @register_tool("goal.add_slave", ['general', 'task'])
@register_tool("goal_add_slave", ['general', 'task'])
def add_slave(instruction: str,
              _meta: _ExecToolMetaData,
              slave_type: SlaveScope = 'general',
              required_results_ids: list[str|Addr] = [],
              slave_name: str|None=None,
              result_name: str|None=None
              ) -> str:
    """
    Adds a step to the task.
    Returns the new slaves address.
    The steps are executed asyncronosly, the moment all of their requirements are resolved. 
    A step may require anouther steps result, by adding the required results name or address. 
    A step gets the results it requires when it is executed.
    Each step is an separate instruction, to be executed, to produce a result, and to pass the result to the next step.
    required_results_ids are for RESULTS OF SLAVES, **NOT RESULTS OF TOOL CALLS**.
    You can assume top down execution of the tool calls you wrote, but asynchronous execution of the slave goals themself.
    slave_type is the type of the slave being added. The differenses are the tools that it sees. There is a baseline of what tools each one sees, and tools only specialists see.
    required_results_names can include "self", wich would mean the currently executed slave, e.g. your current result will be forwarded to it.
    Currently allowed slave_types are: 
    """
    conn = _meta.conn

    required_results_addrs = []

    required_results_addrs = resolve_to_addrs(
        resolve_self(_meta.slave_id, required_results_ids, conn),
        conn
    )
    
    if slave_type == "planner":
        """ This is here as a fallback for a fairly common AI hallucination. Dont remove. """
        return add_replanner_slave(_meta)

    addr = conn.execute_fetchval("""
    SELECT new_slave(
        p_master_addr := %s,
        p_instruction := %s,
        p_name := %s,
        p_requires := %s,
        p_result_name := %s,
        p_slave_scope := %s
    );
        """, 
    (_meta.master_id, instruction, slave_name, required_results_addrs, result_name, slave_type))
    return str(addr)
    #return "Added a new slave"

add_slave.__doc__ = "".join([str(add_slave.__doc__) , "[ " ,  str(get_args(SlaveScope)) , " ]" , "."])



# def add_replanner_slave(_meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("goal.add_planner_slave", ['task'])
@register_tool("goal_add_planner_slave", ['task'])
def add_replanner_slave(_meta: _ExecToolMetaData) -> str:
    """ Adds a planner step, that adds further steps, ensuring the whole plan of the task is created incrementally. TO ADD PLANNER, USE THIS FUNCTION. """
    conn = _meta.conn
    special_context = []
    fetch = conn.execute("""
    SELECT instruction FROM masters WHERE addr = %s;
                         """, (_meta.master_id,)).fetchone()
    assert fetch is not None
    special_context.extend(fetch)

    fetch = conn.execute("""
    SELECT s.instruction, r.content_str FROM masters m JOIN slaves s ON s.master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_meta.master_id,)).fetchall()
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

    master_result_so_far_str = conn.execute("SELECT master_result FROM master_context WHERE addr = %s", (_meta.master_id,)).fetchone()
    master_result_so_far_str = f"Masters result so far: {master_result_so_far_str[0] if master_result_so_far_str is not None else "No master result so far."}"

    fetch = conn.execute("""
    SELECT s.result_addr FROM masters m JOIN slaves s ON master_addr = m.addr JOIN results r ON r.addr = s.result_addr WHERE m.addr = %s;
                         """, (_meta.master_id,)).fetchall()

    # TODO : Enchanse this process by adding a context manager slave as well as better views of previous tasks. 

    prompt  =  """
    You task is to decide how to further proceed. For the master instruction,
    the results and the master result,
    either formulate the next incremental plan step torwards completion of the master instruction,
    or finalise the master result,
    if you already have enough information from the previous steps and their results,
    or do nothing, if the master result is already finalised enough. 
    Also check the quality of instruction and task completion, and if its below acceptance, and if it influences further steps, asign that same task again, to be done anew, and tweak the instruction for higher quality.
    DO NOT ADD SLAVES WITH THE SAME TASK REPETETIVELY!!!
    DO NOT TRY TO PLAN ALL STEPS AT ONCE.
    The task is complete if the master instruction is fully answered via the current master result. 
    """

    prompt = prompt + special_context_str + master_result_so_far_str

    conn.execute("SELECT new_slave(%s, %s, NULL, %s, NULL, NULL, NULL, 'task');", (_meta.master_id, prompt, [r[0] for r in fetch]))

    return ""
    #return "added a replanner slave"



# def master_result_add(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
# @register_tool("result.add_master_result", ALL)
@register_tool("result_add_master_result", ALL)
def master_result_add(text: str, _meta: _ExecToolMetaData) -> str:
    """
    This funtion writes a result for the whole master, e.g. the task that consists of many slaves.
    Newly written result is appended to the master result, it does not overwrite the result.
    """
    conn = _meta.conn
    conn.execute("""
    UPDATE master_context SET master_result = master_result || %s WHERE addr = %s
                 """, (text, _meta.master_id))

    return ""
    #return "Added a master result."



# def context_window_lands(querry: str, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("context.window.semantic_land", ['context'])
@register_tool("context_window_semantic_land", ['context'])
def context_window_lands(querry: str, _meta: _ExecToolMetaData) -> str:
    """
    Lands a viewing window, or a context window, these are the same thing, based on a semantic querry. 
    A viewing window is a dynamic automatic context window capable of providing you with relevant and highly controllable context
    of relevant knowledge and tools to be executed via tool.execute .
    Very important generally. 
    Returns new anchor addr.
    """
    conn = _meta.conn

    emb = embedder.encode_query(querry)

    if isinstance(emb, ndarray):
        emb = emb.tolist()

    anchor = conn.execute_fetchval("""
    SELECT s_land(%s, %s::vector(768))
                 """, (_meta.master_id, emb))

    return str(anchor)
    #return 'Semantically moved the viewing window anchor.'


# def context_window_land_by_addr(id: Addr|str, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("context.window.land_by_addr", ['context'])
@register_tool("context_window_land_by_addr", ['context'])
def context_window_land_by_addr(id: Addr|str, _meta: _ExecToolMetaData) -> str:
    """
    Lands a viewing window onto an item by id.
    """
    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    try:
        addr_type = conn.execute_fetchval("""
            SELECT type FROM addrs_tables WHERE addr = %s;
                                 """, (addr,))
    except NoValue as e:
        raise psycopg.DataError(f"Couldnt resolve addr {addr} to type, due to the following error: {e}, as result of fetch is not subscriptable.")
    if addr_type == "knowledge":
        conn.execute("""
        UPDATE master_context SET
            window_anchor_knowledge = %s,
            window_anchor_exe = NULL,
            window_size_r = 12,
            window_size_l = 12
        WHERE addr = %s;
                     """, (addr, _meta.master_id))

    elif addr_type == "executables":
        conn.execute("""
        UPDATE master_context SET
            window_anchor_exe = %s,
            window_anchor_knowledge = NULL,
            window_size_r = 12,
            window_size_l = 12
        WHERE addr = %s;
                     """, (addr, _meta.master_id))
    else:
        raise psycopg.DataError(f"Invalid addr type gotten. Gotten {addr_type}, expected executables or knowledge.")

    return ""
    #return f"Moved context window center to {addr}"





# @register_tool("context.window.change_size", ['context'])
@register_tool("context_window_change_size", ['context'])
def context_window_size_change(_meta: _ExecToolMetaData, left: int = 0, right: int = 0) -> str:
    """ 
    The function for changing viewing windows size. 
    Negative number shrinks the size, positive number increases the size, possible in one or 2 directions.

    Returns new window size in json format with keys left,
    right for the size to the left and to the right.
    """
    conn = _meta.conn
    
    new = conn.execute("""
    UPDATE master_context
        SET window_size_l = window_size_l + %s, window_size_r = window_size_r + %s
    WHERE addr = %s
    RETURNING window_size_l, window_size_r;
                 """, (left, right, _meta.master_id)).fetchone()
    if not new:
        log_json({
            "type": "syscall",
            "subtype": "context_window_change_size",
            "status": "fatal",
            "msg": "Database querry did not return expected values. Expected (int, int) got None."
        })
        raise RuntimeError("Database querry did not return expected values. Expected (int, int) got None.")

    return '{"left": "' + json.dumps(str(new[0])) + '" , "right": "' + json.dumps(str(new[1])) + '" }' # NOTE : I hate when I cant do fstrings. 
    #return "Changed context window size."


# def move_window_anchor(amount: int, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("context.window.move_anchor", ['context'])
@register_tool("context_window_move_anchor", ['context'])
def move_window_anchor(amount: int, _meta: _ExecToolMetaData) -> str:
    """
    Function to move the anchor of the viewing window.
    Moves to the left if amount if negative, to the right if amount is positive.

    Returns new anchor address.
    """
    conn = _meta.conn

    addr = conn.execute_fetchval("""
    SELECT move_anchor(%s, %s);
                           """, (amount, _meta.master_id))
    
    return str(addr)
    #return "moved context window anchor"



# def result_write(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("result.write", ALL)
@register_tool("result_write", ALL)
def result_write(text: str, _meta: _ExecToolMetaData) -> str:
    """
    Writes plaintext passed in as the result to your current instruction, NOT to the master instruction, NOT to the user. 
    TO MESSAGE USER, USE user.send_message tool!
    """
    return text


# def report_paradoxal_information(items: Sequence[str|Addr], paradox: str, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("K.report_paradoxal_information", ALL)
@register_tool("k_report_paradoxal_information", ALL)
def report_paradoxal_information(items: Sequence[str|Addr], paradox: str, _meta: _ExecToolMetaData) -> str:
    """
    Reports paradoxal items. Items are paradoxal if the information contained withhin them is mutually exclusive.
    paradox: the paradox in the information
    items: the list of items addresses or names that contain the paradoxal information.

    Aborts current execution run.
    Unless you are unable to fulfill your instruction due to Paradox,
    you should add a slave with the task of reporting.
    """

    conn = _meta.conn
    conn.execute("""
    UPDATE results
    SET status = 'paradox',
        status_inf = %s
    FROM slaves s
        JOIN results r ON s.result_addr = r.addr
    WHERE s.addr = %s;
    """, (Jsonb({ 'items': items, 'paradox': paradox }), _meta.slave_id))

    # NOTE : The paradox label is removed when new_result is called by the sql function itself.

    raise ParadoxDetected(paradox, items)




# def add_cronjob(cronjob_type: Literal['once', 'loop'],
#                 action: CronjobActions,
#                 time_between_runs: int,
#                 params: dict[str, Any],
#                 _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("goal.add_cron_job", ['task', 'general'])
@register_tool("goal_add_cron_job", ['task', 'general'])
def add_cronjob(cronjob_type: Literal['once', 'loop'],
                action: CronjobActions,
                time_between_runs: int,
                params: dict[str, Any],
                _meta: _ExecToolMetaData) -> str:
    """
    Spawns a cronjob. The cronjobs can run ether once, if cronjob type is "once", after time_between_runs seconds, or in a loop every time_between_runs seconds indefinetly.
    action is the action that the cronjob should take, out of all the available options.
    params are the params required by the given cronjob_action. The required cronjob_types per action are:
    [
        'do_this_later': {
            "ai_instruction": string // insturction of what to do later.
        }
    ]
    
    More detailed cronjobs documentation available in the knowledge item "cronjobs_documentation"

    Returns cronjob address.
    """
    # TODO : Make the cronjobs_documentation knowledge as part of BaseState.
    addr = insert_cronjob(Cronjob(
            action,
            params,
            cronjob_type,
            time_between_runs
        ),
        _meta.conn
    )
    
    return str(addr)
    #return f"Added a cronjob that does {action}."


# def unload_item(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:

# @register_tool("context.unload_item", ["context"])
@register_tool("context_unload_item", ["context"])
def unload_item(_meta: _ExecToolMetaData, id: Addr|str) -> str:
    """
    Unloads the item from the context window, by id.

    Returns Nothing.
    """
    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    conn.execute("""
    DELETE FROM master_load WHERE master_addr = %s AND item_addr = %s;
                 """, (_meta.master_id, addr))

    return ""
    #return f"Unloaded item {addr}."



# def web_searcher_function_fulltext(query: str, _meta: _ExecToolMetaData, websites_amount: int = 3) -> ActionConfirmation:

# @register_tool("web.search_fulltext", ['general', 'communication'])
@register_tool("web_search_fulltext", ['general', 'communication'])
def web_searcher_function_fulltext(query: str, _meta: _ExecToolMetaData, websites_amount: int = 3) -> str:
    """
    Websearch function that returns fulltext of top websites_amount webpages texts.
    Needs analysis through a second slave for actual anaswer. 

    Full text is xml style tagged. 
    Format: 
        <WEBSITE url=url, title=title, remeinder=LARGE FULLCAPS STRING.>contents</WEBSITE>
    """
    return searcher_obj.search_website_content(query, websites_amount, _meta.context_limit // 2)
    #return f"Websearch for query '{query}', results:'{searcher_obj.search_website_content(query, websites_amount, _meta.context_limit // 2)}'"



# def send_message_to_human_v_webui(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:

# @register_tool("user.send_message", ['general', 'communication'])
# def send_message_to_human_v_webui(text: str, _meta: _ExecToolMetaData) -> str:
#     """
#     Sends a message to the human. Must only be used in presense of an user message, otherwise DONT TOUCH
#     """
#     conn = _meta.conn
#     conn.execute("""
# SELECT new_result(%s, 
#     (SELECT addr FROM results WHERE metadata->>'type'='ai_message' 
#         AND metadata->>'session_name'=(SELECT name FROM names WHERE addr=%s)
#     ORDER BY (metadata->>'turn')::INT ASC LIMIT 1));
#                  """, (text, _meta.master_id))
#     
#     return "Sent a message to the human." NOTE : webui is deprecated and this is broken. Will not fix, will remake.


# def search_for_urls(query: str, amount_results: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
# @register_tool("web.search", ['communication'])
@register_tool("web_search", ['communication'])
def search_for_urls(query: str, amount_results: int, _meta: _ExecToolMetaData) -> str:
    """
    Returns Json list of websearch results in the following structure:
        [
            {
                "url": "url",
                "title": "title",
                "snippet": "snippet"
            },
            {...}, ...
        ]
    """
    results_raw = searcher_obj.search(query)
    results: list[str] = ['[', ']']
    
    for i in results_raw[:amount_results]:
        results.insert(-2, "".join(['{"url": "', json.dumps(i["url"]), '", "title": "', json.dumps(i["title"]), '", "snippet": "', json.dumps(i["snippet"]), '"}']))

    return "".join(results)


#          results.append(f"<website> url={i['url']}, title={i['title']}, snippet={i['snippet']}</website>")
# 
#     if len(results) > 0:
#         return f"websearch results: [{"\n".join(results)}]"
#     else:
#         return f"No results for the websearch of {query}"


# def web_request(url: str,
#                 _meta: _ExecToolMetaData,
#                 timeout: int = 10,
#                 return_type: Literal['extracted', 'raw'] = 'extracted',
#                 headers: dict[str, str] = {}) -> ActionConfirmation:

# @register_tool("web.get", ['general', 'communication'])
@register_tool("web_get", ['general', 'communication'])
def web_request(url: str,
                _meta: _ExecToolMetaData,
                timeout: int = 10,
                return_type: Literal['extracted', 'raw'] = 'extracted',
                headers: dict[str, str] = {}) -> str:
    """
    The GET http request onto the url.
    return_type specifies what you wish to get from that url.
    Extracted means only meaningfull content, and raw means raw response content as string. 
    """
    
    result = httpsystem.get(url, httpx.Headers(headers), timeout)

    if return_type == "extracted":
        return result["text"]
    else:
        return result["content_raw"]

    #return f"<website> content = [{result['text'] if return_type == "extracted" else result['content_raw']}], url = [{result["url"]}], status_code = [{result['status_code']}] </website>"



# def web_post(url: str,
#              _meta: _ExecToolMetaData,
#              timeout: int = 10,
#              return_type: Literal['extracted', 'raw', 'status_code'] = 'extracted',
#              headers: dict[str, str] = {},
#              payload: str = ""
#              ) -> ActionConfirmation:

# @register_tool('web.post', ['communication'])
@register_tool('web_post', ['communication'])
def web_post(url: str,
             _meta: _ExecToolMetaData,
             timeout: int = 10,
             return_type: Literal['extracted', 'raw', 'status_code'] = 'extracted',
             headers: dict[str, str] = {},
             payload: str = ""
             ) -> str:
    """
    The POST http request onto a url.
    return type specifies what you wish to get from that url. 
    Extracted means only meaningfull content, raw means raw response content as string, status_means means no content, only status code.
    """

    result = httpsystem.post(url, httpx.Headers(headers), payload, timeout)

    match return_type:
        case 'status_code':
            return f"<website> url = [{url}], status_code = [{result['status_code']}]</website>"
        case 'extracted':
            return f"<website> url = [{url}], status_code = [{result['status_code']}, content = [{result['text']}]] </website>"
        case 'raw':
            return f"<website> url = [{url}], status_code = [{result['status_code']}, content = [{result['content_raw']}]] </website>"
        case _:
            raise ValueError(f"Invalid input on return type. Input: {return_type}.")



# def create_master(instruction: str,
#                   _meta: _ExecToolMetaData,
#                   required_ids: Sequence[str|Addr] = [],
#                   result_name: str|None = None
#                   ) -> ActionConfirmation:

# @register_tool("goal.add_master", ['task'])
@register_tool("goal_add_master", ['task'])
def create_master(instruction: str,
                  _meta: _ExecToolMetaData,
                  required_ids: Sequence[str|Addr] = [],
                  result_name: str|None = None
                  ) -> str:
    """
    Creates a master goal,
    with the given instruction,
    depending on given results, outputting a given results name.
    Can use "self" to specify the currently executed slave as one of the required_ids.

    Returns master addr
    """

    conn = _meta.conn

    required_ids = resolve_self(_meta.slave_id, required_ids, conn)

    required_addrs = resolve_to_addrs(required_ids, conn)

    addr = conn.execute_fetchval("""
    SELECT new_master(
        p_instruction := %s,
        req_addrs := %s,
        result_name := %s
        );
                 """, (instruction, required_addrs, result_name))

    return str(addr)
    #return f"Created master with instruction '{instruction}'."



# def rmt_create_from_range(_meta: _ExecToolMetaData,
#                           start_id: Addr|str,
#                           end_id: Addr|str,
#                           description: str,
#                           name: str|None = None) -> ActionConfirmation:

# @register_tool("rmt.create.from_range", ['task'])
@register_tool("rmt_create_from_range", ['task'])
def rmt_create_from_range(_meta: _ExecToolMetaData,
                          start_id: Addr|str,
                          end_id: Addr|str,
                          description: str,
                          name: str|None = None) -> str:
    """
    Creates a reusable master template from a range of items. Traverses the live execution history to find the slaves between the start and end, inclusively,
    and then just makes that into an rmt. 
    Does not include any variables, and most likely requires further edits before being usable.
    start_id and end_id may NOT include 'self' or other relative references.
    Description is mandatory because its used for embeddings for position generation and thus for retrievability.

    Returns rmt addr.
    """
    conn = _meta.conn
    addr = create_from_range(start_id, conn, end_id, name)
    
    conn.execute("""
    INSERT INTO vector_ops(addr_rmt, description) VALUES(%s, %s)
                 """, (addr, description))

    return str(addr)
    #return f"Created rmt {name if name is not None else "No name"}@{addr} from range."




# def rmt_serialise(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:

# @register_tool("rmt.serialize", ['task'])
@register_tool("rmt_serialize", ['task'])
def rmt_serialise(_meta: _ExecToolMetaData, id: Addr|str) -> str:
    """
    Serialises an rmt into a readable format.
    Returns JSON:
        {"dsl": "dsl_string", "description": "description_str"}
    """
    conn = _meta.conn
    addr = resolve_to_addr(id, conn)
    serial = serialize(addr, conn)
    description = conn.execute_fetchval("""
    SELECT description FROM vector_ops WHERE addr = %s;
                                        """, (addr,))
    return '{"dsl": "' + json.dumps(serial) + '", "description": "' + json.dumps(description) +'" }'

    #return f"Readable form of RMT {id if isinstance(id, str) else 'No name'}@{addr} with description '{description}': [{serial}]"


# def rmt_create_from_serial(_meta: _ExecToolMetaData, dsl: str, description: str, name: str|None = None) -> ActionConfirmation:

# @register_tool("rmt.create.from_dsl", ['task'])
@register_tool("rmt_create_from_dsl", ['task'])
def rmt_create_from_serial(_meta: _ExecToolMetaData, dsl: str, description: str, name: str|None = None) -> str:
    """
    Creates an rmt from dsl.
    The dsl format is the following: 
        START -> (id='optional node id here', instruction='mandatory instruction unless its a reference', scope='optional slave scope default general') -> (instruction='next slave') -> END
        START -> (id='stuff1', instruction='do stuff1') -> (id='stuff2', instruction='do stuff 2') -> END
            (instruction='do_stuff 1.5') -> (id='stuff2')
            (id='stuff1') -> (instruction='do_stuff 1/2') -> (id='stuff2')
    Rules: 
        START and END dont do anything, they are ignored.
        the dsl structure is basically START -> node_that_does_stuff -> node_that_gets_stuff_to_do_other_stuff -> END
        nodes are inside ()
        they have 3 keyword arguments:
            instruction='' (required)
            id='' (optional)
            scope='' (optional default 'general')
        There are variables that are substituted at the activation time. They are marked like this ${{varname}}.
        Variables are only allowed within instructions.

        There are also **references**.
        References are (id='id that already appeared before'). (parsing order: left to right in lines, top to bottom of the whole input.)
        Note that -> Can not reference through line barriers, e.g. '''
            node -> 
            node2
        '''
        is invalid.

        Intendation is ignored, and whitespaces are ignored.

        References are used to describe branches and merges of the task flow, e.g. when one node is part of many linear execution lines, you define it once, and reference it for the rest of uses.
        During parsing, all references are flattened to just pointers to the node they reference.

        Returns rmt addr.
    """

    conn = _meta.conn
    addr = create_from_serial(dsl, conn, name)

    conn.execute("""
    INSERT INTO vector_ops(addr_rmt, description) VALUES (%s, %s)
                 """, (addr, description))

    return str(addr)
    #return f"Created rmt {name if name is not None else 'No name'}@{addr}."


# def tool_create_from_master(_meta: _ExecToolMetaData,
#                             master_id: Addr|Name,
#                             description: str,
#                             name: str|None = None) -> ActionConfirmation:

# @register_tool("rmt.create.from_master", ['task'])
@register_tool("rmt_create_from_master", ['task'])
def tool_rmt_create_from_master(_meta: _ExecToolMetaData,
                            master_id: Addr|Name,
                            description: str,
                            name: str|None = None) -> str:
    """
    Create rmt from master.
    Does not include any variables, wich means its very likely it will need further edits before being usable.

    returns rmt addr
    """
    conn = _meta.conn
    m_addr = resolve_to_addr(master_id, conn)
    addr = create_from_master(m_addr, conn, name)

    conn.execute("""
    INSERT INTO vector_ops(addr_rmt, description) VALUES(%s, %s)
                 """, (addr, description))
    
    return str(addr)
    #return f"Created rmt from master {master_id if isinstance(master_id, str) else 'No Name'}@{m_addr} under the identifiers {name if name else 'No name'}@{addr}."


# def rmt_edit_description(_meta: _ExecToolMetaData, rmt_id: Addr|Name, new_description: str) -> ActionConfirmation:

# @register_tool("rmt.edit.description", ['task'])
@register_tool("rmt_edit_description", ['task'])
def rmt_edit_description(_meta: _ExecToolMetaData, rmt_id: Addr|Name, new_description: str) -> str:
    """
    Set the rmt description to something new.

    returns Nothing.
    """
    conn = _meta.conn
    
    addr = resolve_to_addr(rmt_id, conn)
    
    occ_check(_meta.occ_last_change, addr, conn, partial(serialize, addr, conn))

    conn.execute("""
    UPDATE vector_ops
        SET description = %s
    WHERE addr = %s;
         """, (new_description, addr))

    update_timestamp(addr, conn)

    return ""
    #return f"Updated description of rmt {rmt_id if isinstance(rmt_id, str) else 'No Name'}@{addr}."
    



# def rmt_delete_node(_meta: _ExecToolMetaData, rmt_slave_id: Addr|Name, template_id: Addr|Name, concatenate: bool = True) -> ActionConfirmation:

# @register_tool("rmt.slave.edit.delete_node", ['task'])
@register_tool("rmt_slave_edit_delete_node", ['task'])
def rmt_delete_node(_meta: _ExecToolMetaData, rmt_slave_id: Addr|Name, template_id: Addr|Name, concatenate: bool = True) -> str:
    """
    Deletes a node from rmt.
    concatenate is a boolean flag that tells if it should concatenate the resulting DAG or not.
    If True, it does this: 
        example: delete node 2
        1 -> 2 -> 3 to 1 -> 3
    if False, it does this:
        example: delete node 2
        1 -> 2 -> 3 to 1 3 (notice no connection between 1 and 3)

    It deletes the node regardless of the rmt template the node belongs to, because it can, so be carefull to remove correct nodes. (Addr and Name are unique, but dont mistype them.)

    Returns Nothing.
    """
    conn = _meta.conn
    
    addr = resolve_to_addr(rmt_slave_id, conn)
    template_addr = resolve_to_addr(template_id, conn)
    
    occ_check(_meta.occ_last_change, template_addr, conn, partial(serialize, template_addr, conn))

    delete_node(addr, conn, concatenate)

    update_timestamp(template_addr, conn)


    return ""
    #return f"Deleted node {rmt_slave_id if isinstance(rmt_slave_id, str) else 'No name'}@{addr} from the rmt."




# def rmt_insert_node(_meta: _ExecToolMetaData,
#                 rmt_id: Addr|Name,
#                 instruction: str,
#                 name: str|None = None,
#                 scope: SlaveScope = 'general',
#                 depends_on: Sequence[ReferenceTo|str] = [],
#                 required_by: Sequence[ReferenceTo|str] = []
#                 ) -> ActionConfirmation:

# @register_tool("rmt.slave.edit.insert_node", ['task'])
@register_tool("rmt_slave_edit_insert_node", ['task'])
def rmt_insert_node(_meta: _ExecToolMetaData,
                rmt_id: Addr|Name,
                instruction: str,
                name: str|None = None,
                scope: SlaveScope = 'general',
                depends_on: Sequence[ReferenceTo|str] = [],
                required_by: Sequence[ReferenceTo|str] = []
                ) -> str:
    """
    Inserts the given node into the given rmt with the given relationships to the reest of the rmt (depends_on, required_by).
    Returns JSON:
        {"rmt_addr": addr, "node_addr": addr}
    """

    conn = _meta.conn
    
    rmt_addr = resolve_to_addr(rmt_id, conn)

    occ_check(_meta.occ_last_change, rmt_addr, conn, partial(serialize, rmt_addr, conn))

    addr = insert_node(rmt_addr, instruction, conn, name, scope, depends_on, required_by)
    
    update_timestamp(rmt_addr, conn)


    return '{"rmt_addr": ' + json.dumps(rmt_addr) +', "node_addr": ' + json.dumps(addr) + '}'
    #return f"Inserted rmt node {name if name else 'No name'}@{addr} into rmt template {rmt_id}."




# def rmt_activate_as_master(_meta: _ExecToolMetaData,
#                            rmt_id: Addr|Name,
#                            inputs: dict[str, str],
#                            depends_on: Sequence[Addr|Name] = [],
#                            required_by: Sequence[Addr|Name] = []
#                            ) -> ActionConfirmation:

# @register_tool("rmt.activate_as_master", ['general', 'task'])
@register_tool("rmt_activate_as_master", ['general', 'task'])
def rmt_activate_as_master(_meta: _ExecToolMetaData,
                           rmt_id: Addr|Name,
                           inputs: dict[str, str],
                           depends_on: Sequence[Addr|Name] = [],
                           required_by: Sequence[Addr|Name] = []
                           ) -> str:
    """
    Activates a reusable master template as a master, with the given relationships to the rest of the task.
    depends_on may use 'self' to identify your current task as a dependancy of the rmt.

    returns the activated masters addr
    """
    conn = _meta.conn
    addr = resolve_to_addr(rmt_id, conn)

    depends_on = resolve_self(_meta.slave_id, depends_on, conn)

    addr = activate_as_master(addr, conn, depends_on, required_by, inputs)

    return str(addr)
    #return f"Activated rmt {rmt_id} as master, with depends_on = {depends_on} and required_by = {required_by}"



# def rmt_edit_instruction(_meta: _ExecToolMetaData, node_id: Addr|Name, sr_block: SearchAndReplaceBlock) -> ActionConfirmation:

# @register_tool("rmt.slave.edit.instruction", ['task'])
@register_tool("rmt_slave_edit_instruction", ['task'])
def rmt_edit_instruction(_meta: _ExecToolMetaData, node_id: Addr|Name, sr_block: SearchAndReplaceBlock) -> ActionConfirmation:
    """
    Edits an rmt_slave's instruction.
    Returns nothing.
    """
    conn = _meta.conn
    
    addr = resolve_to_addr(node_id, conn)

    template_addr = conn.execute_fetchval("""
    SELECT template_addr FROM rmt_slaves WHERE addr = %s;
                                          """, (addr, ))

    occ_check(_meta.occ_last_change, template_addr, conn, partial(serialize, addr, conn))

    edit_instruction(addr, sr_block, conn)

    update_timestamp(template_addr, conn)

    return ""
    #return f"Edited instruction of rmt slave {node_id if isinstance(node_id, str) else 'No name'}@{addr}"


# def rmt_change_scope(_meta: _ExecToolMetaData, node_id: Addr|Name, new_scope: SlaveScope) -> ActionConfirmation:

# @register_tool("rmt.slave.edit.scope", ['task'])
@register_tool("rmt_slave_edit_scope", ['task'])
def rmt_change_scope(_meta: _ExecToolMetaData, node_id: Addr|Name, new_scope: SlaveScope) -> str:
    """
    Changed the scope of an slave in an rmt to the new_scope.

    Returns nothing.
    """
    conn = _meta.conn

    addr = resolve_to_addr(node_id, conn)


    template_addr = conn.execute_fetchval("""
    SELECT template_addr FROM rmt_slaves WHERE addr = %s;
                                          """, (addr, ))

    occ_check(_meta.occ_last_change, template_addr, conn, partial(serialize, addr, conn))

    change_scope(addr, new_scope, conn)

    update_timestamp(template_addr, conn)

    return ""
    #return f"Updated scope of rmt node {node_id}"


# def tool_register_event_reaction_rmt(_meta: _ExecToolMetaData,
#                                 event_path: str,
#                                 rmt_id: Addr|Name,
#                                 args: dict[str, str]) -> ActionConfirmation:

# @register_tool("event.register_reaction.rmt", ['task'])
@register_tool("event_register_reaction_rmt", ['task'])
def tool_register_event_reaction_rmt(_meta: _ExecToolMetaData,
                                event_path: str,
                                rmt_id: Addr|Name,
                                args: dict[str, str]) -> ActionConfirmation:
    """
    Executes an RMT as a callback to the given event.
    "data" and "subject" arguments will be provided additionaly at event arrival time, and filled out with events payload and events full event_path respectfully.
    The event_path provided to the tool is a NATS event subscribtion string.

    Returns consumer addr.
    """
    conn = _meta.conn

    addr = resolve_to_addr(rmt_id, conn)

    addr = register_reaction_rmt(event_path, addr, args, conn)

    return str(addr)
    #return f"Registered callback of rmt {rmt_id if isinstance(rmt_id, str) else 'No name'}@{addr} for event {event_path}."





# def tool_register_event_reaction_execute_slave(
#         _meta: _ExecToolMetaData,
#         event_path: str,
#         instruction: str, 
#         scope: SlaveScope
#         ) -> ActionConfirmation:

# @register_tool("event.register_reaction.slave", ['task'])
@register_tool("event_register_reaction_slave", ['task'])
def tool_register_event_reaction_execute_slave(
        _meta: _ExecToolMetaData,
        event_path: str,
        instruction: str, 
        scope: SlaveScope
        ) -> str:
    """
    Creates a callback slave for the given event path, with the given instruction and given scope. 
    Slaves instruction will have strings ${{data}} and ${{subject}} replaced with the events payload and event path respectfully. 
    The event_path you provide into the tool is NATS event subscribtion string,
    which means the actuall full event type will nearly never be the same you wrote into there.

    Returns consumer addr.
    """
    conn = _meta.conn

    addr = register_reaction_execute_slave(event_path, instruction, scope, conn)

    return str(addr)
    #return f"Registered callback of slave for event  {event_path} with scope {scope}."




# def tool_create_result_via_event(
#         _meta: _ExecToolMetaData,
#         event_path: str, 
#         result_str: str,
#         name: str|None = None
#         ) -> ActionConfirmation:

# @register_tool("event.create_result", ['task'])
@register_tool("event_create_result", ['task'])
def tool_create_result_via_event(
        _meta: _ExecToolMetaData,
        event_path: str, 
        result_str: str,
        name: str|None = None
        ) -> str:
    """
    Creates a result that will be filled out with the event.
    The keys ${{data}} and ${{event}} in the result string will be replaced via the events payload and full event path.
    Event path you provide as an argument is NATS event subscribtion string,
    which means it will nearly never be exactly the same as the actual event path.

    Returns JSON:
        {"result_addr": addr, "consumer_addr": addr}
    """
    conn = _meta.conn
    
    ret = create_result_via_event(event_path, result_str, conn)

    if name:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s);
                     """, (ret.result_addr, name))
    
    return json.dumps(asdict(ret))
    #return f"Created result {name if name is not None else "No Name"}@{ret.result_addr} as result of an event."
    
