#!/usr/bin/env python3

import threading
from types import FunctionType
from typing import Optional, Sequence, TypedDict
from ..interrupts.main import interruptable
from pathlib import Path
from ..utils.config_dir_resolver import config_dir_resolver
import tomllib
from queue import Queue
import json
import httpx
from .types import api, instr_json

config_dir = config_dir_resolver()

config_file = config_dir / "executor.toml"

config = tomllib.loads(config_file.read_text())

executor_queue = Queue()

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
async def core(checkpoint: FunctionType, queue: Queue, apis: Sequence[api]):
    instr = queue.get()
    str_instr = json.dumps(instr)
    for api in apis:
        checkpoint()
        try:
            result = llm_call(api, str_instr)
        except httpx.HTTPStatusError:
            pass
        else:
            break
    





    

for _ in range(config["cores"]):
    
