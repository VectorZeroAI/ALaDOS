#!/usr/bin/env python3
""" Traces types file. """

from typing import TypedDict, NotRequired

from pydantic import JsonValue


class MetadataDagJson_Execution_ToolCall(TypedDict, ):
    id: str
    args: dict[str, JsonValue]
    error: NotRequired[str]

class MetadataDagJson_Execution_SysCall(TypedDict):
    name: str
    args: dict[str, JsonValue]
    error: NotRequired[str]

class MetadataDagJson_Execution(TypedDict):
    tool_calls: list[MetadataDagJson_Execution_ToolCall]
    syscalls: list[MetadataDagJson_Execution_SysCall]
    error: NotRequired[str]

class MetadataDagJson(TypedDict):
    executions: list[MetadataDagJson_Execution]
