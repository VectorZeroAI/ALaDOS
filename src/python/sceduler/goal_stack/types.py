#!/usr/bin/env python3

from typing import TypeAlias, TypedDict

SlaveAddr: TypeAlias = int
MasterAddr: TypeAlias = int

class SlaveObj(TypedDict):
    """ The slave python or pydantic object """
    addr: SlaveAddr
    instruction: str
    master_addr: MasterAddr
    result_name: str

class WindowDataRaw(TypedDict):
    master_addr: MasterAddr
    window_position: int
    window_size_r: int 
    window_size_l: int
