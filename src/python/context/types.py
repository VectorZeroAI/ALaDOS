#!/usr/bin/env python3

from dataclasses import dataclass
from typing import TypeAlias, TypedDict, List, Optional

from ...executor.types import SlaveScope
from ...types import ReferenceTo, ValidTables

SlaveAddr: TypeAlias = int
MasterAddr: TypeAlias = int

@dataclass(slots=True)
class SlaveObj:
    """ The slave python or pydantic object """
    addr: SlaveAddr
    instruction: str
    master_addr: MasterAddr
    result_name: Optional[str]
    scope: SlaveScope

@dataclass(slots=True)
class Anchor:
    ref_addr: ReferenceTo
    ref_table: ValidTables

@dataclass(slots=True)
class WindowData:
    master_addr: MasterAddr
    window_position: Anchor
    window_size_r: int
    window_size_l: int

@dataclass(slots=True)
class LoadsData:
    items_addrs: List[ReferenceTo]

@dataclass(slots=True)
class KnowledgeItem:
    addr: ReferenceTo
    name: Optional[str]
    content: str

@dataclass(slots=True)
class ExecutablesItem:
    addr: ReferenceTo
    name: Optional[str]
    header: str
    body: str

@dataclass(slots=True)
class LogsItem:
    addr: ReferenceTo
    name: Optional[str]
    created_at: int
    action: str
    created_by: int

@dataclass(slots=True)
class MasterAsItem:
    addr: ReferenceTo
    name: Optional[str]
    instruction: str
    result_addr: ReferenceTo
    result_name: Optional[str]
    # NOTE : may later be expanded to include all of context and loads that a master goal has as well.

@dataclass(slots=True)
class SlaveAsItem:
    addr: ReferenceTo
    name: Optional[str]
    master_addr: ReferenceTo
    instruction: str
    result_addr: ReferenceTo
    result_name: Optional[str]

@dataclass(slots=True)
class ResultItem:
    addr: ReferenceTo
    name: Optional[str]
    content_str: str
    ready: bool
