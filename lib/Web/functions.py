#!/usr/bin/env python3
"""
Async client for web‑related syscalls (search, HTTP requests).
"""

import json

from .._.main import call


async def search_fulltext(slave_addr: int, query: str, websites_amount: int = 3) -> str:
    """
    Performs a web search and returns the full text of the top N pages.
    The result is XML‑tagged.
    """
    return await call(
        "web_search_fulltext",
        slave_addr,
        {"query": query, "websites_amount": websites_amount},
    )


async def search(slave_addr: int, query: str, amount_results: int) -> list[dict[str, str]]:
    """
    Performs a web search and returns a JSON list of results
    with 'url', 'title', and 'snippet'.
    """
    result = await call(
        "web_search",
        slave_addr,
        {"query": query, "amount_results": amount_results},
    )
    return json.loads(result)


async def get(
    slave_addr: int,
    url: str,
    timeout: int = 10,
    return_type: str = "extracted",
    headers: dict[str, str] = {},
) -> str:
    """
    Performs an HTTP GET request.
    `return_type` can be 'extracted' (text content) or 'raw'.
    Returns the response content.
    """
    return await call(
        "web_get",
        slave_addr,
        {
            "url": url,
            "timeout": timeout,
            "return_type": return_type,
            "headers": headers,
        },
    )


async def post(
    slave_addr: int,
    url: str,
    timeout: int = 10,
    return_type: str = "extracted",
    headers: dict[str, str] = {},
    payload: str = "",
) -> str:
    """
    Performs an HTTP POST request.
    `return_type` can be 'status_code', 'extracted', or 'raw'.
    Returns the appropriate part of the response.
    """
    return await call(
        "web_post",
        slave_addr,
        {
            "url": url,
            "timeout": timeout,
            "return_type": return_type,
            "headers": headers,
            "payload": payload,
        },
    )
