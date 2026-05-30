#!/usr/bin/env python3

from typing import Sequence
from ..utils.conn_factory import conn_factory
from ..types import ReferenceTo
from .dsl import parse

def serialize(addr: ReferenceTo) -> str:
    """ Serialises a workflow into an structured text representation for the llm. """
    pass

def create_from_serial(expression: str, name: str|None = None) -> ReferenceTo:
    """ Creates a workflow from a serial expression of one. Basically DSL for workflows. """
    parsed = parse(expression)
    conn = conn_factory()
    return conn.execute("SELECT save_rmt(p_parsed_rmt := %s)", (parsed,)).fetchone()[0]


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

def activate_inline(depends_on: Sequence[int|str] = [], required_by: Sequence[int|str] = [], rmt_addr: ReferenceTo) -> None:
    """ Inline inserts the workflow into the DAG. """
    pass

def activate_as_master(depends_on: Sequence[int|str] = [], required_by: Sequence[int|str] = [], rmt_addr: ReferenceTo) -> None:
    """ Activates the workflow as a dedicated master. Encapsulated insdead of inlined. """
    pass
