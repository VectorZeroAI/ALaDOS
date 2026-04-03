#!/usr/bin/env python3

import asyncio
import queue
import threading
from typing import Callable, Coroutine, Sequence, Any
from .queue import executor_interrupt_queue
from ..interrupts.main import interruptable
from ..utils.config_dir_resolver import config_dir_resolver
from ..utils.llm_to_json import llm_to_json
from ..uqueue import global_interrupt_queue
import tomllib
from queue import Queue
import httpx
from .types import api, instr_json, tool_call
import psycopg2
import psycopg2.extensions

def execute_instruction(slave_json: instr_json) -> None:
    executor_queue.put(slave_json)

def _llm_call_claude(api: api, prompt: str) -> str:
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

def _llm_call_openai(api: api, prompt: str) -> str:
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
                    "messages": prompt
                    }
                )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def llm_call(api: api, prompt: str) -> str:
    if api.get('claude'):
        return _llm_call_claude(api, prompt)
    else:
        return _llm_call_openai(api, prompt)

@interruptable(executor_interrupt_queue, global_interrupt_queue) # TODO : figure out how to get global interrupt chanell working and get it to work.
async def core(
        checkpoint: Callable[[], Coroutine[Any, Any, None]],
        queue: Queue[instr_json],
        apis: Sequence[api],
        conn: psycopg2.extensions.connection
        ) -> None:
    while True:
        await checkpoint()
        instr = queue.get()

        str_instr = " ".join((instr["context"], instr["instruction"]))
        
        for api_sps in apis:
            await checkpoint()
            try:
                result = llm_call(api_sps, str_instr)
                await checkpoint()
            except httpx.HTTPStatusError:
                await checkpoint()
                pass
            else:
                await checkpoint()
                break
        else:
            print("All the APIS failed") # TODO : ADD AN RECOVERY INTERRUPT OR ANY FORM OF ERROR RECOVERY
            raise RuntimeError("All the APIS failed")
        tool_calls = llm_to_json(result)
        for call in tool_calls:
            




def core_thread(coroutine, queue: Queue, apis: Sequence[api], conn: psycopg2.extensions.connection) -> None:
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coroutine(queue, apis, conn)) # This is valid, because checkpoint is part of the interruptable decorator, and is injected at decoration time. 
    except Exception as e:
        print(f"CORE THREAD ERRORED OUT: {e}")
    finally:
        loop.close()

def startup(conn: psycopg2.extensions.connection) -> None:
    """ The startup function that starts up the whole executor system """
    global executor_queue
    executor_queue = Queue()

    config_dir = config_dir_resolver()
    config_file = config_dir / "executor.toml"
    config = tomllib.loads(config_file.read_text())

    for _ in range(config["cores_number"]):
        threading.Thread(
                target=core_thread,
                args=(core, executor_queue, config["apis"], conn),
                daemon=True
                ).start()
    print("startup of the executor finished")


"""

config structure is the following: 
    executor.toml,
    sceduler.toml,
    db.toml,
    permissions.toml
    main.toml
    IO.toml
    etc.


An example of an expected executor.toml is:

´´´
cores_number = 12
[[apis]]
url = "http://example.com/v1/completions/example"
key = "example-api-key"
model = "IAMSTUPIDMODELEXAMPLE"
[[apis]]
url = "lazy me"
key = "lazy me"
model = sonnet
claude = true
´´´

"""
