#!/usr/bin/env python3

from typing import TypedDict, Optional

class api(TypedDict):
    url: str
    key: str
    model: str
    claude: Optional[bool]

class instr_json(TypedDict):
    result_addr: int
    instruction: str
    master_addr: int


