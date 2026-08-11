#!/usr/bin/env python3
"""
The internal library main.py file, containing the main logic of the library.

This is the python alados library, containing all of the system calls code required for the
    DB side to be able to efficiently call kernel functions.

This just means that this file will house the transport layer of the entire library of syscalls.
"""
from dataclasses import dataclass
from typing import Any

from nats.aio.client import Client
import json

from nats.aio.msg import Msg

from ALaDOS.src.python.utils.connect_nats import connect_nats # pyright: ignore

async def call(function_name: str, slave_addr: int, args: dict[str, Any]) -> str:
    """ Executes syscall and returns result. """
    nt: Client = await connect_nats()

    reply = await nt.request(
        f"_.syscall.{function_name}",
        json.dumps(args).encode(),
        5
    )

    return reply.data.decode()

@dataclass(slots=True, frozen=True)
class syscall:
    function_name: str
    args: dict[str, Any]

async def batch_call(syscalls: list[syscall], slave_id: int) -> list[str]:
    """ Batch syscalls and batch returns. Order is preserved. """
    nt: Client = await connect_nats()
    response: list[Msg] = []
    for i in syscalls:
        r = await nt.request(f"_.syscall.{slave_id}.{i.function_name}", json.dumps(i.args).encode(), timeout=20)
        response.append(r)
    
    results: list[str] = []

    for i in response:
        results.append(
            i.reply
        )

    return results

