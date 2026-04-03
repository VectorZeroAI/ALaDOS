#!/usr/bin/env python3

import asyncio
import threading
from types import FunctionType
from typing import Sequence
from ..interrupts.main import interruptable
from ..utils.config_dir_resolver import config_dir_resolver
import tomllib
from queue import Queue
import json
import httpx
from .types import api, instr_json
import psycopg2
import psycopg2.extensions


def execute(slave_json: dict): # TODO : ADD a slave json typed dict
    executor_queue.put(slave_json)


def _llm_call_claude(api: api, instr: str) -> str:
    raise NotImplementedError("claude format not implemented yet!") # TODO: IMPLEMENT
#    with httpx.Client() as client:
#        response = client.post(
#                url=api["url"],
#                headers={"Authorization": f"Bearer {api["key"]}"},
#                json={
#                    "model": api["model"],
#                    "messages": instr
#                    }
#                )
#        response.raise_for_status()
#        return response.json()["choices"][0]["message"]["content"]

def _llm_call_openai(api: api, instr: str) -> str:
    """
    This is the function that calls an openai compatable endpoint.  
    THE FULL ENDPOINT URL MUST BE PROVIDED, INCLUDING THE v1/completions whatever path.
    """
    with httpx.Client() as client:
        response = client.post(
                url=api["url"],
                headers={"Authorization": f"Bearer {api["key"]}"},
                json={
                    "model": api["model"],
                    "messages": instr
                    }
                )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def llm_call(api: api, instr: str) -> str:
    if api.get('claude'):
        return _llm_call_claude(api, instr)
    else:
        return _llm_call_openai(api, instr)

@interruptable()
async def core(checkpoint: FunctionType, queue: Queue, apis: Sequence[api], conn: psycopg2.extensions.connection):
    while True:
        await checkpoint()
        instr = queue.get()
        str_instr = json.dumps(instr)
        for api in apis:
            await checkpoint()
            try:
                result = llm_call(api, str_instr)
                await checkpoint()
            except httpx.HTTPStatusError:
                await checkpoint()
                pass
            else:
                await checkpoint()
                break

def core_thread(coroutine: , queue: Queue, apis: Sequence[api], conn:psycopg2.extensions.connection):
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coroutine(queue))

def startup(conn: psycopg2.extensions.connection):
    global executor_queue
    executor_queue = Queue()

    config_dir = config_dir_resolver()
    config_file = config_dir / "executor.toml"
    config = tomllib.loads(config_file.read_text())

    core_thread = threading.Thread(target=core, args=(executor_queue, config["apis"], conn), daemon=True).start()
    for _ in range(config["cores_number"]):
