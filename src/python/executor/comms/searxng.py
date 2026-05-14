#!/usr/bin/env python3
from typing import TypeAlias
import httpx

SearchResultText: TypeAlias = str

def search(querry: str, amount_websites: int = 5) -> SearchResultText:
    """
    The search function that returns the text of the top n websites in the result,
    parsed via Beautiful Soup,
    and injected with anti prompt injection attack patterns.
    """
    with httpx.Client() as client:
        


