#!/usr/bin/env python3

import asyncio
import threading
from typing import Callable, Coroutine, Sequence, Any

from python.executor.execute_tool import execute_tool
from python.utils.conn_factory import conn_factory
from .queue import executor_interrupt_queue
from ..interrupts.main import interruptable
from ..utils.config_dir_resolver import config_dir_resolver
from ..utils.llm_to_json import llm_to_json
from ..queue import global_interrupt_queue
from ..utils.uqueue import Uqueue
import tomllib
import httpx
from .types import api, instr_json, tool_call, tool_calls_block
import psycopg
from .queue import executor_queue

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
                    "messages": [
                            {
                                "role": "system", 
                                "content": prompt
                            }
                        ]
                    }
                )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def llm_call(api: api, prompt: str) -> str:
    if api.get('claude'):
        return _llm_call_claude(api, prompt)
    else:
        return _llm_call_openai(api, prompt)


@interruptable(executor_interrupt_queue, global_interrupt_queue)
async def core(
        checkpoint: Callable[[], Coroutine[Any, Any, None]],
        queue: Uqueue[instr_json],
        apis: Sequence[api],
        ) -> None:

    await checkpoint()
    conn = conn_factory()

    while True:
        await checkpoint()
        instr = await queue.get()

        str_instr = " ".join((instr["context"], instr["instruction"]))
        
        for api_sps in apis:
            await checkpoint()
            try:
                llm_response = llm_call(api_sps, str_instr)
                await checkpoint()
            except httpx.HTTPStatusError:
                await checkpoint()
                pass
            else:
                await checkpoint()
                break
        else:
            print("All the APIS failed") # TODO : ADD AN RECOVERY INTERRUPT OR ANY FORM OF ERROR RECOVERY
            global_interrupt_queue.put("STOP")
        await checkpoint()
        print(llm_response) # FIXME : REmove the debug print statement after done debugging this
        tool_calls: tool_calls_block = llm_to_json(llm_response)

        results = []

        for call in tool_calls:
            await checkpoint()
            results.append(execute_tool(call, instr["master_addr"]))

        result_str = "\n".join(str(results))

        await checkpoint()
        conn.execute("""
        UPDATE results SET ready = TRUE, content_str = %s WHERE addr = %s
                     """, (result_str, instr["result_addr"]))
        

def core_thread(coroutine, queue: Uqueue, apis: Sequence[api]) -> None:
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coroutine(queue, apis)) # This is valid, because checkpoint is part of the interruptable decorator, and is injected at decoration time. 
    except Exception as e:
        print(f"CORE THREAD ERRORED OUT: {e}")
    finally:
        loop.close()

def startup() -> None:
    """ The startup function that starts up the whole executor system """

    config_dir = config_dir_resolver()
    config_file = config_dir / "executor.toml"
    config = tomllib.loads(config_file.read_text())

    for _ in range(config["cores_number"]):
        threading.Thread(
                target=core_thread,
                args=(core, executor_queue, config["apis"]),
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
