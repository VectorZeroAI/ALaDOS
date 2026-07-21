#!/usr/bin/env python3
"""
Context file, the file that handles the entirety of context resolution,
for some reason, in this completely illogically named place. 
I will definetly refactor this crappy directory structure.
And also split this file.
At some point. 
"""

from python.rmt.main import serialize
from python.types import ReferenceTo

from ...executor.execute_tool import HEADERS_REGISTRY
from ...utils.conn_factory import Conn
from .types import Anchor, LoadsData, SlaveObj, WindowData


def resolve_context(slave_obj: SlaveObj, conn: Conn) -> str:
    """
    The main resolve context function that returns the entire context for a slave. 
    Resolves all the crap you would ever need and slaps into a single blob of text. 
    Prompt enginiering subject to improvement.
    """

    window_data = conn.execute("""
    SELECT window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l FROM master_context WHERE addr = %s;
                 """, (slave_obj.master_addr,)).fetchone()
    if window_data is not None:
        
        if not (window_data[0] is None and window_data[1] is None):

            window_data = WindowData(
                slave_obj.master_addr,
                Anchor(
                    window_data[0] if window_data[0] is not None else window_data[1],
                    "executables" if window_data[0] is not None else "knowledge"
                ),
                window_data[3],
                window_data[2]
            )

            window_context = resolve_window(window_data, conn)
        else:
            window_context = "VIEW WINDOW DOES NOT YET EXIST"
    else:
        window_context = "VIEW WINDOW DOES NOT YET EXIST."

    load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = %s;
                             """, (slave_obj.master_addr,)).fetchall()

    if len(load_data) != 0:

        loads_data = LoadsData(
            [addr[0] for addr in load_data]
        )

        load_context = resolve_loads(loads_data, conn)
    else:
        load_context = "NO ITEMS LOADED YET"

    results_context = resolve_req_results(slave_obj, conn)

    TOOL_HEADERS = HEADERS_REGISTRY[slave_obj.scope]

    return "\n\n\n".join([
        f"Current viewing window is: [{window_context}]",
        f"Currently loaded items are: [{load_context}]",
        f"Previous steps results are: [{results_context}]",
        f"Tool headers are: [{TOOL_HEADERS}]",
        f"Your current type is '{slave_obj.scope}'. Other slave types will have other tools available."
    ])



def resolve_req_results(slave_obj: SlaveObj, conn: Conn):
    """
    Resolves the required results of a slave to their
    content_strings concated all into a single string blob.
    """
    req_results_addrs = conn.execute("""
    SELECT req_addr FROM slave_req WHERE slave_addr = %s;
                             """, (slave_obj.addr, )).fetchall()
    
    list_req_results_addrs = [addr[0] for addr in req_results_addrs]
    if len(list_req_results_addrs) < 1:
        return "NO REQUIRED RESULTS PRESENT" # TODO : Insdead of saying no ... present maybe just ignore that?

    
    fetch = conn.execute("""
    SELECT r.content_str, s.instruction, r.metadata FROM results r LEFT JOIN slaves s ON s.result_addr = r.addr WHERE r.addr = ANY(%s)
                         """, (list_req_results_addrs,)).fetchall()
    req_results_str_list = []
    for i in fetch:
        if not i[1]:
            req_results_str_list.append(f"(information required by your instruction '{i[0]} ', with metadata: '{i[2]}')") ## TODO: Make this prompt enginiering not suck. This sucks. 
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
            case 'reusable_master_templates':
                result_str.append(_rmt_item_resolve(addr, conn))
            case 'rmt_slaves':
                result_str.append(_rmt_slaves_item_resolve(addr, conn))
            case 'cronjob_once':
                result_str.append(_cronjob_once_item_resolve(addr, conn))
            case 'cronjob_loop':
                result_str.append(_cronjob_loop_item_resolve(addr, conn))
            case _:
                raise ValueError(f"Database returned a non existant or invalid table name. Returned {table}, but its not a valid table name. If it is, please add that tables case to the above handler.")

    return "".join(result_str)


def _cronjob_loop_item_resolve(addr: ReferenceTo, conn: Conn) -> str:
    item = conn.execute("""
    SELECT cj.body cj.args cj.execute_every, cj.last_ran, cj.error, cj.error_text, n.name
    FROM cronjob_loop cj
        LEFT JOIN names n ON n.addr = cj.addr
    WHERE cj.addr = %s
                        """, (addr, )).fetchone()
    if item is None:
        return f"Item at address {addr} with type cronjob_once does not exist!"

    return f"Cronjob_loop at {item[7] if item[7] is not None else 'No name'}@{addr} With body: '{item[0]}', args: '{item[1]}', execute_every: '{item[2]}', last_ran: '{item[3]}', {f"error: '{item[4]}', with error message: " if item[4] is not None else ''}{f"'{item[5]}'" if item[5] is not None else ''}"
    ## TODO : Refactor this long ass string into something that makes more sense,
    ## together with fixing all of the prompt enginiering going on here
    ## This place fucking sucks!


def _cronjob_once_item_resolve(addr: ReferenceTo, conn: Conn) -> str:
    item = conn.execute("""
    SELECT cj.body, cj.args, cj.start_after, cj.finished, cj.error, cj.error_text, n.name
    FROM cronjob_once cj
        LEFT JOIN names n ON n.addr = cj.addr
    WHERE cj.addr = %s
                        """, (addr, )).fetchone()
    if item is None:
        return f"Item at address {addr} with type cronjob_once does not exist!"

    return f"Cronjob_once at {item[6] if item[6] is not None else 'No name'}@{addr} With body: '{item[0]}', args: '{item[1]}', start_after: '{item[2]}', finished: '{item[3]}', {f"error: '{item[4]}', with error message: " if item[4] is not None else ''}{f"'{item[5]}'" if item[5] is not None else ''}"
    ## TODO : Refactor this long ass string into something that makes more sense,
    ## together with fixing all of the prompt enginiering going on here
    ## This place fucking sucks!




def _rmt_slaves_item_resolve(addr: ReferenceTo, conn: Conn) -> str:
    item = conn.execute("""
    SELECT rmt_s.instruction, rmt_s.addr, n.name, rmt_s.deps
    FROM rmt_slaves rmt_s
        LEFT JOIN names n ON n.addr = rmt_s.addr
    WHERE rmt_s.addr = %s;
                        """, (addr, )).fetchone()

    if item is None:
        return f"Item at address {addr} does not exist." 
    ## TODO : Make it auto remove from context in the DB possibly?
    ## Make an issue on that.

    return f"RMT slave {item[2]}@{item[1]} with instruction: '{item[0]}', dependancies: '{item[3]}'."


def _rmt_item_resolve(addr: ReferenceTo, conn: Conn) -> str:
    item_meta = conn.execute("""
    SELECT n.name, v.description, rmt.addr
    FROM reusable_master_templates rmt
        LEFT JOIN names n ON n.addr = rmt.addr
        INNER JOIN vector_ops v ON v.addr = rmt.addr
    WHERE rmt.addr = %s
                             """, (addr, )).fetchone()
    assert item_meta is not None

    serial = serialize(addr, conn)
    
    return f"Reusable Master Template {item_meta[0] if item_meta[0] is not None else 'No name'}@{item_meta[2]} with description '{item_meta[1]}': content [{serial}]."


def _result_item_resolve(addr: int, conn: Conn) -> str:
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

    result = "@".join((item[0], f"{addr}")) # TODO : LEGIT FIX THIS SHIT MAN ! THIS PROMPT ENGINIERING SUCKS ASS !
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
        ## TODO : FIX. THIS SHIT SUCKS MAN, make the context look nicer!

    return context_str
