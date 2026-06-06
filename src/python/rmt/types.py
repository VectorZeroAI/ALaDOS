#!/usr/bin/env python3

from typing import TypedDict, TypeAlias

class RmtNode(TypedDict):
    instruction: str
    id: str
    deps: list[str]
    index: int

class RmtNodeIncomplete(RmtNode, total=False):
    pass

class RmtNodeReturn(TypedDict):
    instruction: str
    id: str
    deps: list[str]

ParsedRmtExpression: TypeAlias = list[RmtNode]
ReturnParsedRmtExpression: TypeAlias = list[RmtNodeReturn]
