#!/usr/bin/env python3
from dataclasses import dataclass, field

from typing import TypeAlias
from uuid import uuid4
from ..executor.types import SlaveScope_

Name: TypeAlias = str

@dataclass(slots=True)
class RmtNode:
    instruction: str
    id: str
    deps: list[str]
    index: int
    scope: SlaveScope_

@dataclass(slots=True)
class RmtNodeIncomplete:
    index: int = field()
    instruction: str = field(default='')
    deps: list[str] = field(default_factory=list)
    scope: SlaveScope_ = field(default='general')
    id: str = field(default_factory=lambda: str(uuid4()))

@dataclass(slots=True)
class RmtNodeReturn:
    instruction: str
    id: str
    deps: list[str]
    scope: SlaveScope_

ParsedRmtExpression: TypeAlias = list[RmtNode]
ReturnParsedRmtExpression: TypeAlias = list[RmtNodeReturn]
