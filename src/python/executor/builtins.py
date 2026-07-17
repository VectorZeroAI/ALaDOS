#!/usr/bin/env python3

import os
from typing import Any, Literal, Mapping, Sequence, TypeAlias, get_args

from numpy import ndarray
import psycopg
from psycopg.types.json import Jsonb
from python.utils.conn_factory import NoValue
from python.utils.name_resolver import resolve_to_addr, resolve_to_addrs
from .execute_tool import register_tool
import subprocess
import json
from .embedder import embedder
from .types import _ExecToolMetaData, ReferenceTo, SlaveScope
from .exceptions import ParadoxDetected
from .cronjobs.parser import CronjobActions, parse
from .comms.searxng import SearxngSearcher
from .comms import httpsystem
from ..utils.sr_edit import _sr_block_parser, SearchAndReplaceBlock
from ..rmt.main import activate_as_master, change_scope, create_from_range, create_from_serial, delete_node, edit_instruction, insert_node, serialize, create_from_master


Addr: TypeAlias = ReferenceTo
Name: TypeAlias = str
ActionConfirmation: TypeAlias = str

ALL = get_args(SlaveScope)

searcher_obj = SearxngSearcher()

@register_tool("K.create", ['general', 'context'])
def k_create(content: str, description: str, _meta: _ExecToolMetaData, name: str|None = None) -> ActionConfirmation:
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

    if name is not None:
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s);", (addr,name))

    _meta._embedder_queue.put(addr)

    return f"knowledge entry {name if name is not None else "No name"}@{addr} was created."



@register_tool("K.edit", ['general', 'context'])
def k_edit(description_change: SearchAndReplaceBlock,
           content_change: SearchAndReplaceBlock,
           _meta: _ExecToolMetaData,
           id: Addr|str,
           ) -> ActionConfirmation:
    """
    Edits a knowledge entry. 
    Either addr or name must be provided
    change is in the same format as tool.edits change format.

    Also the function signature is this way because I didnt find a better way
    to make addr and name optional while keeping _meta the last argument.
    """
    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    try:
        flag_ownership = conn.execute_fetchval("""
        SELECT TRUE FROM ownership WHERE addr = %s AND owner = %s
                                      """, (addr, _meta.master_id))
    except NoValue:
        flag_ownership = False

    if not flag_ownership:
        raise RuntimeError(f"Item at addr {addr} is not claimed by you, wich means you cant edit it. Claim the item first before editing.")

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

    return f"Edited the knowledge item {id if isinstance(id, str) else "Nameless"}@{addr}"





@register_tool("K.read", ['general', 'context'])
def k_read(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:
    """ Resolve knowledge item by ID. """
    conn = _meta.conn
    addr = resolve_to_addr(id, conn)

    result = conn.execute_fetchval("""
    SELECT content FROM knowledge WHERE addr = %s
                 """, (addr,))

    return f"Knowledge entry {id if isinstance(id, str) else "no name"}@{addr}, contents: {result}."



@register_tool("tool.execute", ['general'])
def execute_tool_builtin_func(_meta: _ExecToolMetaData, id: Addr|str, timeout: int = 10, kwargs: dict|None=None) -> ActionConfirmation:
    """ 
    Executes a tool beyond buildins, from the database, by id.
    One of addr or name must not be None. 
    kwargs are the parameters you pass to the programm. They are json serialised, so do not try to pass in anything other then json.
    Timeout is the execution timeout, e.g. after how much time to kill the process and call it a failiure, in seconds.

    """
    conn = _meta.conn
    
    v_addr = resolve_to_addr(id, conn)

    body = conn.execute_fetchval("""
    SELECT body FROM executables WHERE addr = %s;
                        """, (v_addr,))
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
def create_tool(description: str, header: str, body: str, _meta: _ExecToolMetaData, name: str|None = None) -> ActionConfirmation:
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

    return f"Created tool {name or description}@{addr}"



@register_tool("tool.edit", ['general', 'context'])
def edit_tool(_meta: _ExecToolMetaData,
              id: str|Addr,
              header_change: SearchAndReplaceBlock|None = None,
              body_change: SearchAndReplaceBlock|None = None,
              new_description: str|None = None,
              ) -> ActionConfirmation:
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

    try:
        flag_ownership = conn.execute_fetchval("""
            SELECT TRUE FROM ownership WHERE addr = %s AND owner = %s
                                  """, (addr, _meta.master_id))
    except NoValue:
        flag_ownership = False

    if not flag_ownership:
        raise RuntimeError(f"Item at addr {addr} is not claimed by you, wich means you cant edit it. Claim the item first before editing.")


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

    return f"Applied the edits to the tool {id if isinstance(id, str) else 'No_Name'}@{addr}"




@register_tool("context.add", ['general', 'context'])
def context_add(id: Addr|str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """ Adds an item to the context by addr or by Name. Addr or Name must be provided. Items of any type may be added via this function. """
    conn = _meta.conn
    
    addr = resolve_to_addr(id, conn)
    
    conn.execute("""
    INSERT INTO master_load(master_addr, item_addr) VALUES (%s, %s)
                 """, (_meta.master_id, addr))
    return f"Added context {id if isinstance(id, str) else "No name"}@{addr}."
    # TODO: Try to find a name and insert the name if found.




@register_tool("goal.add_slave", ['general', 'task'])
def add_slave(instruction: str,
              _meta: _ExecToolMetaData,
              slave_type: SlaveScope = 'general',
              required_results_ids: list[str|Addr]|None=None,
              slave_name: str|None=None,
              result_name: str|None=None
              ) -> ActionConfirmation:
    """
    Adds a step to the task. The steps are executed asyncronosly, the moment all of their requirements are resolved. 
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

    if required_results_ids is not None:
        for i in reversed(required_results_ids):
            if i == 'self':
                required_results_ids.remove(i)
                required_results_addrs.append(_meta.slave_id)

        required_results_addrs.extend(resolve_to_addrs(required_results_ids, conn))

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
    (_meta.master_id, instruction, slave_name, required_results_addrs, result_name, slave_type))
    return "Added a new slave"

add_slave.__doc__ = "".join([str(add_slave.__doc__) , "[ " ,  str(get_args(SlaveScope)) , " ]" , "."])

@register_tool("goal.add_planner_slave", ['task'])
def add_replanner_slave(_meta: _ExecToolMetaData) -> ActionConfirmation:
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

    special_context_str = f"Task instruction: {special_context.pop(0)[0]}"

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
    You task is to decide how to further proceed. For a given task,
    the given results and the master results,
    either formulate the next plan steps,
    or finalise the master result, if you already have enough information from the previous steps and their results,
    or do nothing, if the master result is already finalised enough. 
    DO NOT ADD SLAVES WITH THE SAME TASK REPETETIVELY!!!
    DO NOT TRY TO PLAN ALL STEPS AT ONCE.
    The task is complete if the master instruction is fully answered via the current master result. 
    """

    prompt = prompt + special_context_str + master_result_so_far_str

    conn.execute("SELECT new_slave(%s, %s, NULL, %s, NULL, NULL, NULL, 'task');", (_meta.master_id, prompt, [r[0] for r in fetch]))
    return "added a replanner slave"




@register_tool("result.add_master_result", ALL)
def master_result_add(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    This funtion writes a result for the whole master, e.g. the task that consists of many slaves.
    Newly written result is appended to the master result, it does not overwrite the result.
    """
    conn = _meta.conn
    conn.execute("""
    UPDATE master_context SET master_result = master_result || %s WHERE addr = %s
                 """, (text, _meta.master_id))
    return "Added a master result."




@register_tool("context.window.semantic_land", ['context'])
def context_window_lands(querry: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Lands a viewing window, or a context window, these are the same thing, based on a semantic querry. 
    A viewing window is a dynamic automatic context window capable of providing you with relevant and highly controllable context
    of relevant knowledge and tools to be executed via tool.execute .
    Very important generally. 
    """
    conn = _meta.conn

    emb = embedder.encode_query(querry)

    if isinstance(emb, ndarray):
        emb = emb.tolist()

    conn.execute("""
    SELECT s_land(%s, %s::vector(768))
                 """, (_meta.master_id, emb))
    return 'Semantically moved the viewing window anchor.'




@register_tool("context.window.land_by_addr", ['context'])
def context_window_land(id: Addr|str, _meta: _ExecToolMetaData) -> ActionConfirmation:
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
    return f"Moved context window center to {addr}"





@register_tool("context.window.change_size", ['context'])
def context_window_size_change(_meta: _ExecToolMetaData, left: int = 0, right: int = 0) -> ActionConfirmation:
    """ 
    The function for changing viewing windows size. 
    Negative number shrinks the size, positive number increases the size, possible in one or 2 directions.
    """
    conn = _meta.conn
    
    conn.execute("""
    UPDATE master_context SET window_size_l = window_size_l + %s, window_size_r = window_size_r + %s WHERE addr = %s;
                 """, (left, right, _meta.master_id))
    return "Changed context window size."




@register_tool("context.window.move_anchor", ['context'])
def move_window_anchor(amount: int, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Function to move the anchor of the viewing window.
    Moves to the left if amount if negative, to the right if amount is positive.
    """
    conn = _meta.conn

    conn.execute("""
    SELECT move_anchor(%s, %s);
                           """, (amount, _meta.master_id))
    return "moved context window anchor"





@register_tool("result.write", ALL)
def result_write(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Writes plaintext passed in as the result to your current instruction, NOT to the master instruction, NOT to the user. 
    TO MESSAGE USER, USE user.send_message tool!
    """
    return f"Result: {text}"




@register_tool("K.report_paradoxal_information", ALL)
def report_paradoxal_information(items: Sequence[str|Addr], paradox: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Reports paradoxal items. Items are paradoxal if the information contained withhin them is mutually exclusive.
    paradox: the paradox in the information
    items: the list of items addresses or names that contain the paradoxal information.
    """

    conn = _meta.conn
    conn.execute("""
    UPDATE results
    SET status = 'paradox',
        status_inf = %s
    FROM slaves s
    WHERE s.addr = %s;
    """, (Jsonb({ 'items': items, 'paradox': paradox }), _meta.slave_id))
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
    }, _meta.conn)

    return f"Added a cronjob doing {cronjob_action}"




@register_tool("context.unload_item", ["context"])
def unload_item(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:
    """
    Unloads the item from the context window, by id.
    """
    conn = _meta.conn

    addr = resolve_to_addr(id, conn)

    conn.execute("""
    DELETE FROM master_load WHERE master_addr = %s AND item_addr = %s;
                 """, (_meta.master_id, addr))

    return f"Unloaded item {addr}."





@register_tool("web.search_fulltext", ['general', 'communication'])
def web_searcher_function_fulltext(query: str, _meta: _ExecToolMetaData, websites_amount: int = 3) -> ActionConfirmation:
    """
    Websearch function that returns fulltext of top websites_amount webpages texts. Needs analysis through a second slave for actual anaswer. 
    """
    return f"Websearch for query '{query}', results:'{searcher_obj.search_website_content(query, websites_amount, _meta.context_limit // 2)}'"





@register_tool("user.send_message", ['general', 'communication'])
def send_message_to_human_v_webui(text: str, _meta: _ExecToolMetaData) -> ActionConfirmation:
    """
    Sends a message to the human. Must only be used in presense of an user message, otherwise DONT TOUCH
    """
    conn = _meta.conn
    conn.execute("""
SELECT new_result(%s, 
    (SELECT addr FROM results WHERE metadata->>'type'='ai_message' 
        AND metadata->>'session_name'=(SELECT name FROM names WHERE addr=%s)
    ORDER BY (metadata->>'turn')::INT ASC LIMIT 1));
                 """, (text, _meta.master_id))
    
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

    if len(results) > 0:
        return f"websearch results: [{"\n".join(results)}]"
    else:
        return f"No results for the websearch of {query}"




@register_tool("web.get", ['general', 'communication'])
def web_request(url: str,
                _meta: _ExecToolMetaData,
                timeout: int = 10,
                return_type: Literal['extracted', 'raw'] = 'extracted',
                headers: Sequence[Mapping[str, str]] = []) -> ActionConfirmation:
    """
    The GET http request onto the url.
    return_type specifies what you wish to get from that url.
    Extracted means only meaningfull content, and raw means raw response content as string. 
    """
    
    result = httpsystem.get(url, headers, timeout) 

    return f"<website> content = [{result['text'] if return_type == "extracted" else result['content_raw']}], url = [{result["url"]}], status_code = [{result['status_code']}] </website>"




@register_tool('web.post', ['communication'])
def web_post(url: str,
             _meta: _ExecToolMetaData,
             timeout: int = 10,
             return_type: Literal['extracted', 'raw', 'status_code'] = 'extracted',
             headers: Sequence[Mapping[str, str]] = [],
             payload: str = ""
             ) -> ActionConfirmation:
    """
    The POST http request onto a url.
    return type specifies what you wish to get from that url. 
    Extracted means only meaningfull content, raw means raw response content as string, status_means means no content, only status code.
    """

    result = httpsystem.post(url, headers, payload, timeout)

    match return_type:
        case 'status_code':
            return f"<website> url = [{url}], status_code = [{result['status_code']}]</website>"
        case 'extracted':
            return f"<website> url = [{url}], status_code = [{result['status_code']}, content = [{result['text']}]] </website>"
        case 'raw':
            return f"<website> url = [{url}], status_code = [{result['status_code']}, content = [{result['content_raw']}]] </website>"
        case _:
            raise ValueError("Invalid input on return type. Input: {return_type}.")

@register_tool("goal.add_master", ['task'])
def create_master(instruction: str,
                  _meta: _ExecToolMetaData,
                  required_ids: Sequence[str|Addr] = [],
                  result_name: str|None = None
                  ) -> ActionConfirmation:
    """
    Creates a master goal, with the given instruction, depending on given results, outputting a given results name.
    """

    conn = _meta.conn

    required_addrs = resolve_to_addrs(required_ids, conn)

    conn.execute("""
    SELECT new_master(
        p_instruction := %s,
        req_addrs := %s,
        result_name := %s
        );
                 """, (instruction, required_addrs, result_name))

    return f"Created master with instruction '{instruction}'."

@register_tool("claim_item", ['general', 'context'])
def claim_item(_meta: _ExecToolMetaData, item_id: Addr|str) -> ActionConfirmation:
    """
    Before editing an item, you must claim it with this function.
    You can only suply item_name OR item_addr, not both, not none.
    This function claims you to be the owner of the item, so only you can edit the file.
    """
    conn = _meta.conn
    item_addr = resolve_to_addr(item_id, conn)

    conn.execute("""
    INSERT INTO ownership(addr, owner) VALUES(%s, %s)
                 """, (item_addr, _meta.master_id))

    return f"Claimed the item at address {item_addr}."


@register_tool("release_item", ['general', 'context'])
def release_item(_meta: _ExecToolMetaData, item_id: Addr|str) -> ActionConfirmation:
    """
    Function to release the file, allowing others to edit the file, after you no longer need the item. Make sure to release the items you claimed when you no longer need them.
    """
    conn = _meta.conn

    item_addr = resolve_to_addr(item_id, conn)

    conn.execute("""
    DELETE FROM ownership WHERE addr = %s AND owner = %s;
                 """, (item_addr,_meta.master_id))

    return f"Released the item at addr {item_addr}"



@register_tool("rmt.create.from_range", ['task'])
def rmt_create_from_range(_meta: _ExecToolMetaData, start_id: Addr|str, end_id: Addr|str, name: str|None = None) -> ActionConfirmation:
    """
    Creates a reusable master template from a range of items. Traverses the live execution history to find the slaves between the start and end, inclusively,
    and then just makes that into an rmt. 
    Does not include any variables, and most likely requires further edits before being usable.
    start_id and end_id may NOT include 'self' or other relative references.
    """
    conn = _meta.conn
    addr = create_from_range(start_id, conn, end_id, name)
    return f"Created rmt {name if name is not None else "No name"}@{addr} from range."


@register_tool("rmt.serialize", ['task'])
def rmt_serialise(_meta: _ExecToolMetaData, id: Addr|str) -> ActionConfirmation:
    """
    Serialises an rmt into a readable format.
    """
    conn = _meta.conn
    addr = resolve_to_addr(id, conn)
    serial = serialize(addr, conn)
    return f"Readable form of RMT {id if isinstance(id, str) else 'No name'}@{addr} : [{serial}]"


@register_tool("rmt.create.from_dsl", ['task'])
def rmt_create_from_serial(_meta: _ExecToolMetaData, dsl: str, name: str|None = None) -> ActionConfirmation:
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
    """

    conn = _meta.conn
    addr = create_from_serial(dsl, conn, name)
    return f"Created rmt {name if name is not None else 'No name'}@{addr}."



@register_tool("rmt.create.from_master", ['task'])
def tool_create_from_master(_meta: _ExecToolMetaData, master_id: Addr|Name, name: str|None = None) -> ActionConfirmation:
    """
    Create rmt from master.
    Does not include any variables, wich means its very likely it will need further edits before being usable.
    """
    conn = _meta.conn
    m_addr = resolve_to_addr(master_id, conn)
    addr = create_from_master(m_addr, conn, name)
    
    return f"Created rmt from master {master_id if isinstance(master_id, str) else 'No Name'}@{m_addr} under the identifiers {name if name else 'No name'}@{addr}."


@register_tool("rmt.edit.delete_node", ['task'])
def rmt_delete_node(_meta: _ExecToolMetaData, node_id: Addr|Name, concatenate: bool = True) -> ActionConfirmation:
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
    """
    conn = _meta.conn
    delete_node(node_id, conn, concatenate)
    return f"Deleted node {node_id} from the rmt."


@register_tool("rmt.edit.insert_node", ['task'])
def rmt_insert_node(_meta: _ExecToolMetaData,
                rmt_id: Addr|Name,
                instruction: str,
                name: str|None = None,
                scope: SlaveScope = 'general',
                depends_on: Sequence[ReferenceTo|str] = [],
                required_by: Sequence[ReferenceTo|str] = []
                ) -> ActionConfirmation:
    """
    Inserts the given node into the given rmt with the given relationships to the reest of the rmt (depends_on, required_by).
    """

    conn = _meta.conn
    addr = insert_node(rmt_id, instruction, conn, name, scope, depends_on, required_by)
    
    return f"Inserted rmt node {name if name else 'No name'}@{addr} into rmt template {rmt_id}."



@register_tool("rmt.activate_as_master", ['general', 'task'])
def rmt_activate_as_master(_meta: _ExecToolMetaData,
                           rmt_id: Addr|Name,
                           inputs: dict[str, str],
                           depends_on: Sequence[Addr|Name] = [],
                           required_by: Sequence[Addr|Name] = []
                           ) -> ActionConfirmation:
    """
    Activates a reusable master template as a master, with the given relationships to the rest of the task.
    depends_on may use 'self' to identify your current task as a dependancy of the rmt.
    """
    conn = _meta.conn
    addr = resolve_to_addr(rmt_id, conn)

    activate_as_master(addr, conn, depends_on, required_by, inputs)

    return f"Activated rmt {rmt_id} as master, with depends_on = {depends_on} and required_by = {required_by}"


@register_tool("rmt.edit.instruction", ['task'])
def rmt_edit_instruction(_meta: _ExecToolMetaData, node_id: Addr|Name, sr_block: SearchAndReplaceBlock) -> ActionConfirmation:
    """
    Edits the rmt instruction.
    """
    conn = _meta.conn
    edit_instruction(node_id, sr_block, conn)
    return f"Edited instruction of rmt node {node_id}"


@register_tool("rmt.edit.scope", ['task'])
def rmt_change_scope(_meta: _ExecToolMetaData, node_id: Addr|Name, new_scope: SlaveScope) -> ActionConfirmation:
    """
    Updates the new_scope
    """
    conn = _meta.conn
    change_scope(node_id, new_scope, conn)
    return f"Updated instruction of rmt node {node_id}"
