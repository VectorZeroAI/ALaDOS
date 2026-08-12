#!/usr/bin/env python3
"""
The types for this subsystem.
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, TypeAlias, Union

from nats.aio.client import Client
from nats.aio.msg import Msg

from ..executor.types import JsonSerializable
from ..utils.conn_factory import conn_factory_raw

virtual_addr_counter: int = 0

def real_new_addr() -> int:
    """ This creates a real DB level internal new address. """
    conn = conn_factory_raw()
    val = conn.execute_fetchval("SELECT new_internal_addr();")
    conn.close()
    return val

def virtual_new_addr() -> int:
    """
    This just returns a new addr from a virtual counter. 
    This means this can be re used across startups to always get the exact same addr for exact same object,
    and when you do need a real addr you insert a real one.
    """
    global virtual_addr_counter
    virtual_addr_counter =+ 1
    return virtual_addr_counter
    

@dataclass(slots=True)
class Knowledge:
    description: str
    content: str
    name: str = field()
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class Executable:
    description: str
    body: str # TODO : Allow passing in a pathlib.Path object.
    header: str # TODO : Refactor the executables to include langauge.
    name: str = field()
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class Logs:
    ... # TODO : Make it if I ever need it.

@dataclass(slots=True)
class Results:
    content_str: str
    metadata: JsonSerializable = field()
    name: str = field()
    addr: int = field(default_factory=virtual_new_addr)
    ready: bool = field(default=False)

@dataclass(slots=True)
class Masters:
    instruction: str
    result_addr: int
    deps: list[int] = field()
    name: str = field()
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class Slaves:
    master_addr: int
    instruction: str
    result_addr: int
    deps: list[int] = field()
    scope: str = field(default="general")
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class Cronjob:
    """
    Unified cronjob type definition. 

    timelapse is ether execute_every or execute_after - now
    """
    type: Literal["once", "loop"]
    timelapse: int
    action_name: str = field()
    args: JsonSerializable = field()
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class Rmt:
    dsl: str
    description: str
    name: str = field()
    addr: int = field(default_factory=virtual_new_addr)

@dataclass(slots=True)
class EventConsumers:
    """
    The Unified Event Consumers type.
    The field1 and 2 are basically the 2 collumns of the table corresponding to action_type
    Order is the definition order in the sql file 1.
    """
    event_path: str
    action_type: Literal["call_rmt", "execute_slave", "fill_result"]
    field1: Any
    field2: Any
    addr: int = field(default_factory=virtual_new_addr)


@dataclass(slots=True)
class CustomConsumer:
    """
    Custom event consumer type.
    Used for inetrnal communications handling, such as syscalls.
    """
    event_path: str
    consumer_inner_callback: Callable[[Msg, Client], None]


Item: TypeAlias = Union[Knowledge, Executable, Results, Masters, Slaves, Cronjob, Rmt, EventConsumers, CustomConsumer]
