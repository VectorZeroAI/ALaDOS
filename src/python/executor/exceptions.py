#!/usr/bin/env python3

"""
This is where exceptions of the executor are stored.
"""

from typing import Sequence


class ParadoxDetected(Exception):
    def __init__(self, paradox_description: str, items: Sequence[str|int]):
        self.paradox: str = paradox_description
        self.items: Sequence[str|int] = items

class ContextLimitExceeded(Exception):
    def __init__(self, payload: str):
        self.payload = payload
        self.len_payload = len(payload)


