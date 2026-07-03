#!/usr/bin/env python3

from typing import Sequence

import psycopg
from psycopg.errors import DataError
from ..utils.conn_factory import conn_factory
from ..types import ReferenceTo
from .dsl import parse, serialise
from psycopg.types.json import Jsonb

def serialize(addr: ReferenceTo) -> str:
    """ Serialises a workflow into an structured text representation for the llm. """
    return serialise(addr)


def create_from_serial(expression: str, name: str|None = None) -> ReferenceTo:
    """ Creates a workflow from a serial expression of one. Basically DSL for workflows. """

    parsed = parse(expression)

    jsonb_parsed = Jsonb(parsed)

    conn = conn_factory()

    return conn.execute("SELECT save_rmt(p_parsed_rmt := %s, p_name := %s)", (jsonb_parsed, name)).fetchone()[0]


def create_from_master(master_addr: ReferenceTo, name: str|None = None) -> ReferenceTo:
    """
    Creates a workflow from a master goal.
    Fully automatically, except it doesnt place variables anywhere.
    """

    SLAVE_INSTR = 0
    SLAVE_ADDR = 1
    SLAVE_SCOPE = 2
    SLAVE_RES_ADDR = 3
    SLAVE_DEPS = 4

    conn = conn_factory()

    rmt_addr = conn.execute("""
    INSERT INTO reusable_master_templates RETURNING addr;
                            """).fetchone()[0]
    
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
        new_addr = conn.execute("SELECT new_addr();").fetchone()[0]
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
             conn.execute("SELECT new_addr();").fetchone()[0]
         )
    )


    conn.execute("""
    INSERT INTO names(name, addr) VALUES(%s, %s)
                 """, (name, rmt_addr))

    return rmt_addr




def create_from_range(addrs_list: Sequence[int|str], name: str|None = None) -> ReferenceTo:
    """ Creates a workflow from a range of slaves. They must be connected to eachother directly via the DAG, else ValueError is raised. """
    pass


def delete_node(node_id: ReferenceTo|str, concatenate: bool = True) -> None:
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
    
    conn = conn_factory()

    with conn.transaction():
        if isinstance(node_id, str):
            try:
                node_id = conn.execute("SELECT resolve_name(%s);", (node_id)).fetchone()[0]
            except TypeError:
                raise NameError("PROVIDED node_id string does not exist as a name!")

        reqired_by = conn.execute("""
        SELECT addr FROM rmt_slaves WHERE %s = ANY(deps);
                                  """, (node_id,)).fetchall()

        reqired_by = [r[0] for r in reqired_by]

        if concatenate:
            requirements = conn.execute("""
            SELECT deps FROM rmt_slaves WHERE addr = %s;
                                        """, (node_id,)).fetchone()[0]
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




def insert_node(rmt_addr: ReferenceTo, instruction: str, name: str|None = None, depends_on: Sequence[int|str] = []) -> None:
    """ Inserts a node into the rmt DAG. """
    pass

# def activate_inline(rmt_addr: ReferenceTo,
#                     depends_on: Sequence[int|str] = [],
#                     required_by: Sequence[int|str] = [],
#                     inputs: Sequence[dict[str, str]] = []) -> None:
#     """ Inline inserts the workflow into the DAG. """
#     pass


def activate_as_master(rmt_addr: ReferenceTo,
                       depends_on: Sequence[int|str] = [],
                       required_by: Sequence[int|str] = [],
                       inputs: Sequence[dict[str, str]] = []) -> None:
    """
    Activates the workflow as a dedicated master. Encapsulated insdead of inlined.

    DETAILS:
        depends_on are backwards facing edges into the DAG
        required_by are the forward facing edges, e.g. they have to be inverse applied.
        inputs are for later when insertable variables come into place.
    """
    conn = conn_factory()

    depends_on = list(depends_on)

    for i in range(len(depends_on)):
        if isinstance(depends_on[i], str):
            name_tuple = conn.execute("SELECT resolve_name(%s);", (depends_on[i],)).fetchone()
            if name_tuple is not None:
                name = name_tuple[0]
            else:
                raise DataError("Provided name was not able to be resolved.")
            depends_on[i] = name

    conn.execute("""
    SELECT activate_rmt_as_master(
            p_rmt_addr := %s,
            p_depends_on := %s,
            p_required_by := %s,
            p_inputs := %s
        );
                 """, (rmt_addr, depends_on, required_by, inputs))
    





