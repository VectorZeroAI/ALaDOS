#!/usr/bin/env python3

"""
The file where all the types are.
"""

from dataclasses import dataclass

@dataclass(slots=True)
class Event:
    event_path: str
    payload: str
    
    def send(self) -> None:
        print("SEND NOT YET IMPLEMENTED IN THE EVENT CLASS!")
        raise NotImplementedError("SEND NOT YET IMPLEMENTED IN THE EVENT CLASS!")
