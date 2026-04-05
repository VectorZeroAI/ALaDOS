#!/usr/bin/env python3

from typing import LiteralString, Optional, TypedDict, TypeAlias, Union, Literal

ValidTables: TypeAlias = Union[Literal['executables'],
                               Literal['knowledge'],
                               Literal['addrs'],
                               Literal['results'],
                               Literal['slaves'],
                               Literal['masters'],
                               Literal[''],
                               ]

class ReferenceTo(TypedDict):
    addr: int
    ref_addr: int
    ref_table: Optional[str]
