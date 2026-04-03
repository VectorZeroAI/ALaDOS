#!/usr/bin/env python3

from typing import TypedDict, Optional

class api(TypedDict):
    """ An api endpoint representation """
    url: str
    key: str
    model: str
    claude: Optional[bool]

class instr_json(TypedDict):
    """ An atomic instruction json representation """
    result_addr: int
    instruction: str
    master_addr: int
    context: str


