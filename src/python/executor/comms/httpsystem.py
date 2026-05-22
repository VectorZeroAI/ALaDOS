#!/usr/bin/env python3

from typing import Sequence, TypedDict
import httpx
from trafilatura import extract

class ResponseObj(TypedDict):
    url: str
    text: str
    status_code: int
    content_raw: str

def get(url: str, headers: Sequence, timeout: int = 5) -> ResponseObj:
    """
    GET http operation
    """
    with httpx.Client(timeout=timeout) as client:
        response = client.get(
            url=url,
            headers=headers
        )

    result: ResponseObj = {
        'url': url,
        'status_code': response.status_code,
        'text': str(extract(response.text)),
        'content_raw': response.text
    }
    return result

def post(url: str, headers: Sequence, payload: str, timeout: int = 5) -> ResponseObj:
    """ POST http operation """
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            url=url,
            headers=headers,
            content=payload
        )
    result: ResponseObj = {
        'url': url,
        'status_code': response.status_code,
        'text': str(extract(response.text)),
        'content_raw': response.text
    }
    return result
