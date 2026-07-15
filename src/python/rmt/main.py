#!/usr/bin/env python3

from dataclasses import asdict
from typing import Sequence

from psycopg.errors import DataError
from python.rmt.types import ReturnParsedRmtExpression, RmtNodeReturn
from ..utils.conn_factory import conn_factory, Conn
from ..types import ReferenceTo
from .dsl import parse, serialise
from psycopg.types.json import Jsonb

import re

def serialize(addr: ReferenceTo, conn: Conn) -> str:
    """ Serialises a workflow into an structured text representation for the llm. """
    return serialise(addr, conn)


def create_from_serial(expression: str, conn: Conn, name: str|None = None) -> ReferenceTo:
    """ Creates a workflow from a serial expression of one. Basically DSL for workflows. """

    parsed = parse(expression)

    jsonb_parsed = Jsonb([asdict(p) for p in parsed])

    return conn.execute_fetchval("SELECT save_rmt(p_parsed_rmt := %s, p_name := %s)", (jsonb_parsed, name))


def create_from_master(master_addr: ReferenceTo, conn: Conn, name: str|None = None) -> ReferenceTo:
    """
    Creates a workflow from a master goal.
    Fully automatically, except it doesnt place variables anywhere.
    """

    SLAVE_INSTR = 0
    SLAVE_ADDR = 1
    SLAVE_SCOPE = 2
    SLAVE_RES_ADDR = 3
    SLAVE_DEPS = 4

    rmt_addr = conn.execute_fetchval("""
    INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr;
                            """)
    
    slaves = conn.execute("""
    SELECT instruction, addr, scope, result_addr FROM slaves WHERE master_addr = %s;
                          """, (master_addr,)).fetchall()

    slave_addrs = [s[SLAVE_ADDR] for s in slaves]

    slaves = [[i for i in s] for s in slaves]
    # NOTE:  This shenanigan transforms List[TupleRow] type into List[List] type.


    """
    Now we also have to remove the planner slaves as they would only hinder the rmt.
    Notice to future me: Expand this section with any sysinternal slaves that were added. 
    """

    names = conn.execute("""
    SELECT name, addr FROM names WHERE addr = ANY(%s)
                         """, (slave_addrs,)).fetchall()

    for n in names:
        if n[0].startswith("planner_"):
            for i in reversed(range(len(slaves))): # Its fine cause I reverse the order, and now I can do this. 
                if slaves[i][SLAVE_ADDR] == n[1]:
                    slaves.pop(i)


    slave_addrs = [s[SLAVE_ADDR] for s in slaves] # Redefine slave addrs for deps to not include already cut out slaves.

    deps_addrs = [s[SLAVE_RES_ADDR] for s in slaves]


    deps = conn.execute("""
    SELECT slave_addr, req_addr FROM slave_req WHERE slave_addr = ANY(%s) AND req_addr = ANY(%s)
                        """, (slave_addrs, deps_addrs)).fetchall()


    deps = [[i for i in dep] for dep in deps]
    # NOTE:  This shenanigan transforms List[TupleRow] type into List[List] type.


    for s in slaves:
        s.append([])


    for s in slaves:
        new_addr = conn.execute_fetchval("SELECT new_addr();")
        for d in deps:
            if d[0] == s[SLAVE_ADDR]:
                d[0] = new_addr

            if d[1] == s[SLAVE_RES_ADDR]:
                d[1] = new_addr


        s[SLAVE_ADDR] = new_addr

    """
    So we moved the addresses into the new ones,
    now we have to go populate the deps field in the slaves list,
    and then we can just write back to the DB.
    """

    for s in slaves:
        for d in deps:
            if d[0] == s[SLAVE_ADDR]:
                s[SLAVE_DEPS].append(d[1])

    """
    Now writeback to DB
    """

    for s in slaves:
        conn.execute("""
    INSERT INTO rmt_slaves(template_addr, deps, addr, instruction, scope, result_addr)
    VALUES(%s, %s, %s, %s, %s, %s)
                     """,
         (
             rmt_addr,
             s[SLAVE_DEPS],
             s[SLAVE_ADDR],
             s[SLAVE_INSTR],
             s[SLAVE_SCOPE],
             conn.execute_fetchval("SELECT new_addr();")
         )
    )


    if name is not None:
        conn.execute("""
        INSERT INTO names(name, addr) VALUES(%s, %s)
                     """, (name, rmt_addr))

    return rmt_addr




def create_from_range(
        start_node_id: str|int,
        conn: Conn,
        end_node_id: str|int,
        name: str|None = None
        ) -> ReferenceTo:
    """ Creates a workflow from a range of slaves. They must be connected to eachother directly via the DAG, else ValueError is raised. """

    if isinstance(start_node_id, str):
        try:
            start_node_id = conn.execute_fetchval("SELECT resolve_name(%s);", (start_node_id,))
            end_node_id = conn.execute_fetchval("SELECT resolve_name(%s);", (end_node_id,))
        except TypeError as e:
            raise ValueError(f"NAME COULD NOT BE RESOLVED, MOST LIKELY. ERROR: {e}")

    forwards_nodes = conn.execute_fetchval("SELECT recursive_walk_forwards_slaves_dag(%s);", (start_node_id,))
    backwards_nodes = conn.execute_fetchval("SELECT recursive_walk_backwards_slaves_dag(%s);", (end_node_id,))
    print(f"forwards_nodes: {forwards_nodes}")
    print(f"backwards nodes: {backwards_nodes}")

    forwards_nodes = [r for r in forwards_nodes]
    backwards_nodes = [r for r in backwards_nodes]

    print(f"forwards_nodes as list: {forwards_nodes}")
    print(f"backwards nodes as list: {backwards_nodes}")

    forwards_nodes = set(forwards_nodes)
    backwards_nodes = set(backwards_nodes)

    intersection: set[int] = forwards_nodes & backwards_nodes # NOTE : This weird sign here is doing the intersection detection work.
    print("intersection addresses:", intersection )
    print("intersection as list: ", [ a for a in intersection])


    """
    Okay, so we have the steps now,
    so what we do is we copy slaves from slaves and slave_req into rmt_slaves,
    as well as init the rmt template in its respective table.
    """

    slaves = conn.execute("""
    SELECT instruction, scope, addr FROM slaves WHERE addr = ANY(%s::BIGINT[])
        """, ([a for a in intersection],))

    deps = conn.execute("""
    SELECT s.addr, sr.slave_addr
    FROM slave_req sr
        JOIN slaves s ON sr.req_addr = s.result_addr
    WHERE sr.slave_addr = ANY(%s::BIGINT[])
                        """, ([a for a in intersection], ))
    
    #slaves_deps: dict[int, list[str]] = {}
    slaves_deps = {}

    for d in deps:
        slaves_deps[d[1]] = []

    for d in deps:
        slaves_deps[d[1]].append(d[0])

    slaves = [[i for i in s] for s in slaves]

    result: ReturnParsedRmtExpression = []

    for s in slaves:
        result.append(RmtNodeReturn(
            s[0],
            s[2],
            slaves_deps[s[2]],
            s[1]
        ))

    result_jsonb: Jsonb = Jsonb([asdict(r)] for r in result)

    return conn.execute_fetchval("SELECT save_rmt(%s, %s);", (result_jsonb, name))


def delete_node(node_id: ReferenceTo|str, conn: Conn, concatenate: bool = True) -> None:
    """
    Deletes a node from an rmt, via the addr or name.
    
    concatenate: If true, it will concatenate the 2 resulting DAGs,
    so that the execution line doesnt get broken, if false,
    wont do anything and just errase the node and all edges to it with it.

    example:
        delete node 2
        rmt:
            (1) -> (2) -> (3)
        result with concatenate = True:
            (1) -> (3)
        result with concatenate = False:
            (1)
            (2)

    """
    
    Conn

    with conn.transaction():
        if isinstance(node_id, str):
            try:
                node_id = conn.execute_fetchval("SELECT resolve_name(%s);", (node_id))
            except TypeError:
                raise NameError("PROVIDED node_id string does not exist as a name!")

        reqired_by = conn.execute("""
        SELECT addr FROM rmt_slaves WHERE %s = ANY(deps);
                                  """, (node_id,)).fetchall()

        reqired_by = [r[0] for r in reqired_by]

        if concatenate:
            requirements = conn.execute_fetchval("""
            SELECT deps FROM rmt_slaves WHERE addr = %s;
                                        """, (node_id,))
            if requirements is None:
                requirements = []

            
            for i in reqired_by:
                for j in requirements:
                    conn.execute("""
                        UPDATE rmt_slaves
                            SET deps = array_append(deps, %s)
                        WHERE addr = %s
                                 """, (j, i))

        conn.execute("""
        DELETE FROM addrs WHERE addr = %s
                     """, (node_id,))


        conn.execute("""
            UPDATE rmt_slaves
                SET deps = array_remove(deps, %s)
            WHERE addr = ANY(%s)
                     """, (node_id, reqired_by))




def insert_node(rmt_addr: ReferenceTo,
                instruction: str,
                conn: Conn,
                name: str|None = None,
                depends_on: Sequence[int|str] = []
                ) -> None:
    """ Inserts a node into the rmt DAG. """
    
    pass




def activate_as_master(rmt_addr: ReferenceTo,
                       conn: Conn,
                       depends_on: Sequence[int|str] = [],
                       required_by: Sequence[int|str] = [],
                       inputs: dict[str, str] = {}) -> None:
    """
    Activates the workflow as a dedicated master. Encapsulated insdead of inlined.

    DETAILS:
        depends_on are backwards facing edges into the DAG
        required_by are the forward facing edges, e.g. they have to be inverse applied.
        inputs are for later when insertable variables come into place.
    """

    depends_on = list(depends_on)

    for i in range(len(depends_on)):
        if isinstance(depends_on[i], str):
            name_tuple = conn.execute("SELECT resolve_name(%s);", (depends_on[i],)).fetchone()
            if name_tuple is not None:
                name = name_tuple[0]
            else:
                raise DataError("Provided name was not able to be resolved.")
            depends_on[i] = name

    master_addr = conn.execute_fetchval("""
        SELECT new_master( p_instruction := 'NONE', req_addrs := %s); 
                               """, (depends_on,)
                               )


    master_result_addr = conn.execute_fetchval("""
        SELECT result_addr FROM masters WHERE addr = %s;
                                      """, (master_addr,))

    curr = conn.cursor()
    curr.executemany("""
    INSERT INTO slave_req(slave_addr, req_addr) VALUES (%s, %s)
                    """, [(i, master_result_addr) for i in required_by])
        
    rmt_template = conn.execute("""
    SELECT addr,
        master_addr,
        instruction,
        result_addr,
        scope, deps
    FROM rmt_slaves
    WHERE template_addr = %s
    """, [rmt_addr,]).fetchall()

    """
    And now its time to translate the deps. So, I need to update the entire thing.
    I will do it naively, cause the thing is,
        if performance of this thing will be bad enough to care,
        I will rewrite into plpgsql, and get some speed there.
    As long as this is not the case, I will just continue with the O(n * n) approach.
    """

    rmt_template = [{
        "addr": t[0],
        "master_addr": t[1],
        "instruction": t[2],
        "result_addr": t[3],
        "scope": t[4],
        "deps": t[5]
        } for t in rmt_template]

    for i in rmt_template:
        old_addr = i['result_addr']

        i['addr']= conn.execute_fetchval("SELECT new_addr()")
        i['result_addr']= conn.execute_fetchval("SELECT new_addr()")

        conn.execute("INSERT INTO results(addr) VALUES(%s)", [i['result_addr'],])

        for j in rmt_template:
            if j['deps'] is None:
                continue
            for indx, k in enumerate(j['deps']):
                if k == old_addr:
                    j['deps'][indx] = i['result_addr']

    """
    In this part here we replace all the placeholders in format of ${{key}} 
    with their value under the key in p_inputs. 
    """

    all_keys = set()
    key_regex_pattern = r'\$\{\{([a-zA-Z0-9_]+)\}\}'

    for i in rmt_template:
        all_keys.update(re.findall(key_regex_pattern, i['instruction']))

    missing_keys = [key for key in all_keys if key not in inputs]
    redundant_keys = [key for key in inputs if key not in all_keys]

    if missing_keys and redundant_keys:
        raise ValueError(f'Keys {missing_keys} are missing from inputs. Additionally, the following redundant keys were found: {redundant_keys}')

    if missing_keys:
        raise ValueError(f'Keys {missing_keys} are missing from inputs.')

#    if redunant_keys:
#        (f'Input keys {redundant_keys} found in inputs but not found in the template. Double check if this is the right template.')
# NOTE : Propably log the thing, but logging should be implemented later.
# TODO: ADD logging

    def replace_match(match):
        key = match.group(1)
        return str(inputs[key])


    for i in rmt_template:
        i['instruction'] = re.sub(key_regex_pattern, replace_match, i['instruction'])

    for i in rmt_template:
        conn.execute("""SELECT new_slave(
            p_master_addr := %s,
            p_instruction := %s,
            p_requires := %s,
            p_result_addr := %s,
            p_slave_scope := %s
        )""", [
            master_addr,
            i['instruction'],
            i['deps'],
            i['result_addr'],
            i['scope']
        ]
    )

    conn.execute("""
    INSERT INTO names(name, addr) VALUES('_rmt_activation'||nextval('global_rmt_activation_serial'), %s)
                 """, (master_addr,))
