#!/usr/bin/env python3

from typing import TypeAlias, Union, Literal

ValidTables: TypeAlias = Union[Literal['executables'],
                               Literal['knowledge'],
                               Literal['addrs'],
                               Literal['results'],
                               Literal['slaves'],
                               Literal['masters'],
                               Literal['slave_req'],
                               Literal['names'],
                               Literal['logs'],
                               Literal['master_context'],
                               Literal['master_load'],
                               ]

ReferenceTo: TypeAlias = int
