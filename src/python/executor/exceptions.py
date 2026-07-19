#!/usr/bin/env python3

"""
This is where exceptions of the executor are stored.
"""

from typing import Sequence

class ParadoxDetected(Exception):
    def __init__(self, paradox_description: str, items: Sequence[str|int]):
        self.paradox: str = paradox_description
        self.items: Sequence[str|int] = items

class ContextLimitExceededError(Exception):
    def __init__(self, payload: str):
        self.payload = payload
        self.len_payload = len(payload)

class ConcurrencyError(Exception):
    def __init__(self, new_item_content: str) -> None:
        self.item_content = new_item_content
    def __str__(self) -> str:
        return f"The item was changed after the context generation state, wich means the edit is not directly applicable to the item. Please review the edit compared to the new contents of the item to determine if its still applicable, and in what form. New item contents: {self.item_content}"
