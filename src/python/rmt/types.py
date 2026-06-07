#!/usr/bin/env python3

from typing import TypedDict, TypeAlias
from ..executor.types import SlaveScope_

class RmtNode(TypedDict):
    instruction: str
    id: str
    deps: list[str]
    index: int
    scope: SlaveScope_

class RmtNodeIncomplete(RmtNode, total=False):
    pass

class RmtNodeReturn(TypedDict):
    instruction: str
    id: str
    deps: list[str]
    scope: SlaveScope_

ParsedRmtExpression: TypeAlias = list[RmtNode]
ReturnParsedRmtExpression: TypeAlias = list[RmtNodeReturn]
