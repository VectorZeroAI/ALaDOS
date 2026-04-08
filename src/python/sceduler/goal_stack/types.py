#!/usr/bin/env python3

from typing import TypeAlias, TypedDict, List, Optional
from ...types import ReferenceTo, ValidTables

SlaveAddr: TypeAlias = int
MasterAddr: TypeAlias = int

class SlaveObj(TypedDict):
    """ The slave python or pydantic object """
    addr: SlaveAddr
    instruction: str
    master_addr: MasterAddr
    result_name: str

class Anchor(TypedDict):
    ref_addr: ReferenceTo
    ref_table: ValidTables

class WindowData(TypedDict):
    master_addr: MasterAddr
    window_position: Anchor
    window_size_r: int
    window_size_l: int

class LoadsData(TypedDict):
    items_addrs: List[ReferenceTo]
    master_addr: MasterAddr

class KnowledgeItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    content: str

class ExecutablesItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    header: str
    body: str

class LogsItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    created_at: int
    action: str
    created_by: int

class MasterAsItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    instruction: str
    result_addr: ReferenceTo
    result_name: Optional[str]
    # NOTE : may later be expanded to include all of context and loads that a master goal has as well.

class SlaveAsItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    master_addr: ReferenceTo
    instruction: str
    result_addr: ReferenceTo
    result_name: Optional[str]

class ResultItem(TypedDict):
    addr: ReferenceTo
    name: Optional[str]
    content_str: str
    ready: bool
