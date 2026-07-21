#!/usr/bin/env python3


from .item_loaders_registry import register_item_loader
from ..utils.conn_factory import Conn
from ..types import ReferenceTo
from ..rmt.main import serialize


@register_item_loader('knowledge')
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

@register_item_loader('executables')
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


@register_item_loader('logs')
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


@register_item_loader('masters')
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

@register_item_loader('slaves')
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


@register_item_loader('results')
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


@register_item_loader('reusable_master_templates')
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


@register_item_loader('rmt_slaves')
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


@register_item_loader('cronjob_once')
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


@register_item_loader('cronjob_loop')
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
