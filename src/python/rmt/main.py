#!/usr/bin/env python3

from typing import Sequence

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
    """ Creates a workflow from a master goal, semi automatically. (a few LLM calls + automatic extraction) """
    pass

def create_from_range(addrs_list: Sequence[int|str], name: str|None = None) -> ReferenceTo:
    """ Creates a workflow from a range of slaves. They must be connected to eachother directly via the DAG, else ValueError is raised. """
    pass


def delete_node(rmt_addr: ReferenceTo, node_id: int|str) -> None:
    """ Deletes a node from an rmt, via the id. The id is ether node name or auto asigned id. Auto asigned ID is defined in the DSL for the workflows. """
    pass

def insert_node(rmt_addr: ReferenceTo, instruction: str, name: str|None = None, depends_on: Sequence[int|str] = []) -> None:
    """ Inserts a node into the rmt DAG. """
    pass

def activate_inline(rmt_addr: ReferenceTo,
                    depends_on: Sequence[int|str] = [],
                    required_by: Sequence[int|str] = [],
                    inputs: Sequence[dict[str, str]] = []) -> None:
    """ Inline inserts the workflow into the DAG. """
    pass

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
                 """)
    





