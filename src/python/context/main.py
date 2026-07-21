#!/usr/bin/env python3
"""
Context file, the file that handles the entirety of context resolution,
for some reason, in this completely illogically named place. 
I will definetly refactor this crappy directory structure.
And also split this file.
At some point. 
"""

from python.context.item_loaders_registry import load_item

from ..rmt.main import serialize
from ..types import ReferenceTo

from ..executor.execute_tool import HEADERS_REGISTRY
from ..utils.conn_factory import Conn
from .types import Anchor, LoadsData, SlaveObj, WindowData


def resolve_context(slave_obj: SlaveObj, conn: Conn) -> str:
    """
    The main resolve context function that returns the entire context for a slave. 
    Resolves all the crap you would ever need and slaps into a single blob of text. 
    Prompt enginiering subject to improvement.
    """

    window_context = resolve_window(slave_obj.master_addr, conn)

    load_context = resolve_loads(slave_obj.master_addr, conn)

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
    


def resolve_loads(master_addr: ReferenceTo, conn: Conn) -> str:
    """
    Resolved loads data for a master based on master addr. Returned a string.
    """

    load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = %s;
                             """, (master_addr,)).fetchall()

    if len(load_data) < 1:
        return 'No items are loaded.'

    loads_data = LoadsData(
        [addr[0] for addr in load_data]
    )

    result_str: list[str] = []
    table_addr = conn.execute("""
    SELECT addr, type FROM addrs_tables WHERE addr = ANY(%s)
        """, (loads_data.items_addrs,)).fetchall()
    # FIXME : Refactor the addrs_tables view into a materialised view, else performance will screw you!

    for i in table_addr:
        result_str.append(load_item(i[0], i[1], conn))

    return "".join(result_str)


def resolve_window(master_addr: ReferenceTo, conn: Conn) -> str:
    """
    This function gets the master addr and conn and resolves stuff to window data.
    If window data is None, it short returns to "WINDOW DOES NOT EXIST YET."
    """
    
    window_data_fetch = conn.execute("""
    SELECT window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l FROM master_context WHERE addr = %s;
                 """, (master_addr,)).fetchone()
    if window_data_fetch is None:
        return "WINDOW DOES NOT EXIST YET."
        
    if window_data_fetch[0] is None and window_data_fetch[1] is None:
        raise ValueError(f"Invalid viewing window. Viewing window fetch: {window_data_fetch}, expected position 0 or 1 to have an addr.")

    window_data = WindowData(
        master_addr,
        Anchor(
            window_data_fetch[0] if window_data_fetch[0] is not None else window_data_fetch[1],
            "executables" if window_data_fetch[0] is not None else "knowledge"
        ),
        window_data_fetch[3],
        window_data_fetch[2]
    )

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
