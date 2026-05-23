#!/usr/bin/env python3

import os
from typing import Any, Literal, Sequence, TypeAlias, get_args

from numpy import ndarray
import psycopg
from psycopg.types.json import Jsonb
from .execute_tool import register_tool
import subprocess
import re
import json
from .embedder import embedder
from .types import _ExecToolMetaData, SlaveScope
from .exceptions import ParadoxDetected
from .cronjobs.parser import CronjobActions, parse
from .comms.searxng import SearxngSearcher

ActionConfirmation: TypeAlias = str
search_and_replace_block: TypeAlias = str

ALL = get_args(SlaveScope)

searcher_obj = SearxngSearcher()

def _sr_block_parser(sr_block: search_and_replace_block) -> tuple[str, str]:
    """
    retuns (search, replacement)
    search and replace blocks outputten by the model parser that retuns a list of strings and their replacements. 
    """
    match = re.search(
        r"<SEARCH>\s*(.*?)\s*</SEARCH>\s*\s*<REPLACE>\s*(.*?)\s*</REPLACE>",
        sr_block,
        re.DOTALL
    )
    if not match:
        raise ValueError("No matches found.")
    
    search = match.group(1).strip()
    replacement = match.group(2)
    return (search, replacement)



@register_tool("K.create", ['general', 'context'])
def k_create(content: str, description: str, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ 
    Creates a knowledge item.
    The description is a short definition of the items contents for semantic similarity search.
    Content is the actual content, and name is name which can be used a access the item.
    Name of a knowledge item CANNOT be used in goal.add_slave required_results_names.
    """
    conn = _meta['conn']

    addr = conn.execute("SELECT new_addr();").fetchone()[0] # pyright: ignore

    conn.execute("""
    INSERT INTO knowledge (addr, content) VALUES (%s, %s);
                 """, (addr, content))
    conn.execute("""
    INSERT INTO vector_ops (addr_k, description) VALUES (%s, %s);
                 """, (addr, description))

    if name is not None:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr,name))

    _meta['_embedder_queue'].put(addr)

    return f"knowledge entry {name if name is not None else "No name"}@{addr} was created."



@register_tool("K.edit", ['general', 'context'])
def k_edit(addr: int|None = None,
           name: str|None = None,
           description_change: search_and_replace_block = None,
           content_change: search_and_replace_block = None,
           _meta: _ExecToolMetaData = None
           ) -> ActionConfirmation:
    """
    Edits a knowledge entry. 
    Either addr or name must be provided
    change is in the same format as tool.edits change format.
    """
    conn = _meta['conn']
    if addr is None:
        addr = conn.execute("""
        SELECT resolve_name(%s);
                     """, (name,)).fetchone()[0]

    if content_change is not None:
        old_k = conn.execute("""
        SELECT content FROM knowledge WHERE addr = %s;
                             """, (addr,)).fetchone()[0]
        assert isinstance(old_k, str)
        search, replace = _sr_block_parser(content_change)
        new_k = old_k.replace(search, replace)
        conn.execute("""
        UPDATE knowledge SET content = %s WHERE addr = %s;
                     """, (new_k, addr))

    if description_change is not None:
        old_d = conn.execute("""
        SELECT description FROM vector_ops WHERE addr = %s;
                             """, (addr,)).fetchone()[0]
        assert isinstance(old_d, str)
        search, replace = _sr_block_parser(description_change)
        new_d = old_d.replace(search, replace)
        conn.execute("""
        UPDATE vector_ops SET description = %s WHERE addr = %s;
                     """, (new_d, addr))

        _meta['_embedder_queue'].put(addr)

    return f"Edited the knowledge item {name if name is not None else "Nameless"}@{addr}"





@register_tool("K.read", ['general', 'context'])
def k_read(addr: int|None = None, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ Reads a knowledge item by address or by name. One of those must be provided. """
    conn = _meta['conn']
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

    return f"Knowledge entry {name if name is not None else "no name"}@{addr if addr is not None else "Did not resolve address."} contents: {result}."



@register_tool("tool.execute", ['general'])
def execute_tool_builtin_func(addr: int|None = None, name: str|None = None, timeout: int = 10, kwargs: dict|None=None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """ 
    Executes a tool beyond buildins, from the database, by address or name.
    One of addr or name must not be None. 
    kwargs are the parameters you pass to the programm. They are json serialised, so do not try to pass in anything other then json.
    Timeout is the execution timeout, e.g. after how much time to kill the process and call it a failiure, in seconds.

    """
    conn = _meta['conn']
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

    return f"ran tools stdout: {result.stdout}" # TODO : add error handling and stderr capturing on error.



@register_tool("tool.create", ['context'])
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
    INSERT INTO executables(header, body, addr) VALUES (%s, %s, %s);
                 """, (header, body, addr,))
    conn.execute("""
    INSERT INTO vector_ops(addr_exe, description) VALUES (%s, %s)
                 """, (addr, description))
    if name is not None:
        conn.execute("""
        INSERT INTO names(addr, name) VALUES(%s, %s);
                     """, (addr, name))

    _meta['_embedder_queue'].put(addr)

    return f"Created tool {name or description}@{addr}"



@register_tool("tool.edit", ['general', 'context'])
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
        UPDATE vector_ops SET description = %s WHERE addr = %s;
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




@register_tool("context.add", ['general', 'context'])
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




@register_tool("goal.add_slave", ['general', 'task'])
def add_slave(instruction: str,
              slave_type: SlaveScope = 'general',
              required_results_names: list[str]|None=None,
              required_results_addrs: list[int]|None=None,
              slave_name: str|None=None,
              result_name: str|None=None,
              _meta: _ExecToolMetaData=None) -> ActionConfirmation:
    """
    Adds a step to the task. The steps are executed asyncronosly, the moment all of their requirements are resolved. 
    A step may require anouther steps result, by adding the required results name or address. 
    A step gets the results it requires when it is executed.
    Each step is an separate instruction, to be executed, to produce a result, and to pass the result to the next step.
    required_results_names and required_results_addrs are for RESULTS OF SLAVES, not RESULTS OF TOOL CALLS.
    You can assume top down execution of the tool calls you wrote, but asynchronous execution of the slave goals themself.
    slave_type is the type of the slave being added. The differenses are the tools that it sees. There is a baseline of what tools each one sees, and tools only specialists see.
    required_results_names can include "self", wich would mean the currently executed slave, e.g. your current result will be forwarded to it.
    Currently allowed slave_types are: 
    """
    conn = _meta['conn']
    if required_results_addrs is None:
        required_results_addrs = []

    if required_results_names is not None:
        for i in required_results_names:
            if i == "self":
                required_results_addrs.append(_meta['slave_id'])
                continue
            required_results_addrs.append(conn.execute("""
            SELECT resolve_name(%s);
                  """, (i,)).fetchone()[0])

    if slave_type == "planner":
        return add_replanner_slave(_meta) # NOTE: Dont remove this, the AI will continue to fuck this up forever

    conn.execute("""
    SELECT new_slave(
        p_master_addr := %s,
        p_instruction := %s,
        p_name := %s,
        p_requires := %s,
        p_result_name := %s,
        p_slave_scope := %s
    );
        """, 
    (_meta['master_id'], instruction, slave_name, required_results_addrs, result_name, slave_type))
    return "Added a new slave"

add_slave.__doc__ = "".join([str(add_slave.__doc__) , "[ " ,  str(get_args(SlaveScope)) , " ]" , "."])

@register_tool("goal.add_planner_slave", ['task'])
def add_replanner_slave(_meta: _ExecToolMetaData) -> ActionConfirmation:
    """ Adds a planner step, that adds further steps, ensuring the whole plan of the task is created incrementally. TO ADD PLANNER, USE THIS FUNCTION. """
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

    special_context_str = f"Task instruction: {special_context.pop(0)[0]}"

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

    conn.execute("SELECT new_slave(%s, %s, NULL, %s, NULL, NULL, NULL, 'task');", (_meta['master_id'], prompt, [r[0] for r in fetch]))
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




@register_tool("context.window.semantic_land", ['context'])
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




@register_tool("context.window.land_by_addr", ['context'])
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





@register_tool("context.window.change_size", ['context'])
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




@register_tool("context.window.move_anchor", ['context'])
def move_window_anchor(amount: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Function to move the anchor of the viewing window.
    Moves to the left if amount if negative, to the right if amount is positive.
    """
    conn = _meta['conn']

    conn.execute("""
    SELECT move_anchor(%s, %s);
                           """, (amount, _meta['master_id']))
    return "moved context window anchor"





@register_tool("result.write", ALL)
def result_write(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Writes plaintext passed in as the result to your current instruction, NOT to the master instruction.
    """
    return f"Result: {text}"




@register_tool("K.report_paradoxal_information", ALL)
def report_paradoxal_information(items: Sequence[str|int], paradox: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Reports paradoxal items. Items are paradoxal if the information contained withhin them is mutually exclusive.
    paradox: the paradox in the information
    items: the list of items addresses or names that contain the paradoxal information.
    """

    conn = _meta['conn']
    conn.execute("""
    UPDATE results
    SET status = 'paradox',
        status_inf = %s
    FROM slaves s
    WHERE s.addr = %s;
    """, (Jsonb({ 'items': items, 'paradox': paradox }), _meta['slave_id']))
    raise ParadoxDetected(paradox, items)




@register_tool("goal.add_cron_job", ['task', 'general'])
def add_cronjob(cronjob_type: Literal['once', 'loop'],
                cronjob_action: CronjobActions,
                time_between_runs: int,
                params: dict[str, Any],
                _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Spawns a cronjob. The cronjobs can run ether once, if cronjob type is "once", after time_between_runs seconds, or in a loop every time_between_runs seconds indefinetly.
    cronjob_action is the action that the cronjob should take, out of all the available options.
    params are the params required by the given cronjob_action. The required cronjob_types per action are:
    [
        'do_this_later': {
            "ai_instruction": string // insturction of what to do later.
        }
    ]
    
    """
    parse({
        "action": cronjob_action,
        "cronjob_type": cronjob_type,
        "params": params,
        "run_after_or_every_s": time_between_runs
    })
    return f"Added a cronjob doing {cronjob_action}"




@register_tool("context.unload_item", ["context"])
def unload_item(addr: int|None = None, name: str|None = None, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """
    addr or name must be given.
    """
    if not addr and not name:
        raise TypeError("addr or name must be provided")

    conn = _meta['conn']

    if not addr:
        addr = conn.execute("""
    SELECT resolve_name(%s);
                            """, (name,)).fetchone()[0]

    conn.execute("""
    DELETE FROM master_load WHERE master_addr = %s AND item_addr = %s;
                 """, (_meta['master_id'], addr))

    return f"Unloaded item {addr}."





@register_tool("web.search_fulltext", ['general', 'communication'])
def web_searcher_function_fulltext(query: str, websites_amount: int = 3, _meta: _ExecToolMetaData = None) -> ActionConfirmation:
    """
    Websearch function that returns fulltext of top websites_amount webpages texts. Needs analysis through a second slave for actual anaswer. 
    """
    return f"Websearch for query '{query}', results:'{searcher_obj.search_website_content(query, websites_amount, _meta['context_limit'] // 2)}'"





@register_tool("user.send_message", ['general', 'communication'])
def send_message_to_human_v_webui(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Sends a message to the human. Must only be used in presense of an user message, otherwise DONT TOUCH
    """
    conn = _meta['conn']
    conn.execute("""
SELECT new_result(%s, 
    (SELECT addr FROM results WHERE metadata->>'type'='ai_message' 
        AND metadata->>'session_name'=(SELECT name FROM names WHERE addr=%s)
    ORDER BY (metadata->>'turn')::INT ASC LIMIT 1));
                 """, (text, _meta['master_id']))
    
    return "Sent a message to the human."


@register_tool("web.search", ['communication'])
def search_for_urls(query: str, amount_results: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Returns the normal websearch result like structure.
    """
    results_raw = searcher_obj.search(query)
    results: list[str] = []
    for i in results_raw[:amount_results]:
         results.append(f"<website> url={i['url']}, title={i['title']}, snippet={i['snippet']}</website>")
    if len(results) > 1: 
        return "\n".join(results)
    else: 
        return results





