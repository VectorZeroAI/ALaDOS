#!/usr/bin/env python3

import asyncio
import json
import re
import threading
import tomllib
from time import sleep
from typing import Any, Callable, Coroutine, Sequence

import httpx

from ..executor.execute_tool import execute_tool
from ..interrupts.main import interruptable
from ..queue import global_interrupt_queue
from ..sceduler.goal_stack.context import HEADERS_REGISTRY
from ..types import ReferenceTo
from ..utils.config_dir_resolver import config_dir_resolver
from ..utils.conn_factory import conn_factory
from ..utils.llm_to_json import llm_to_json
from ..utils.logger import log_json
from ..utils.uqueue import Uqueue
from . import embedder
from .queue import embedder_queue
from .queue import executor_interrupt_queue, executor_queue
from .types import _exec_tool_meta_data, api, instr_json, tool_calls_block

config_dir = config_dir_resolver()
config_file = config_dir / "executor.toml"
config = tomllib.loads(config_file.read_text())

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
    THE FULL ENDPOINT URL MUST BE PROVIDED, INCLUDING THE v1/chat/completions whatever path.
    """
    with httpx.Client(timeout=60) as client:
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
                        ],
                    "max_completion_tokens": api.get('max_tokens', 4096),
                    "max_tokens": api.get('max_tokens', 4096)
                    }
                )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]


def llm_call(api: api, prompt: str) -> str:
    if api.get('claude'):
        return _llm_call_claude(api, prompt)
    else:
        return _llm_call_openai(api, prompt)



def llm_call_with_ratelimit(api: api, prompt: str) -> str:
    """
    The function that encapsulates the entire logic of the llm call with rate limit in itself.
    """

    try:
        sleep(api.get('rate_limit') if api.get('rate_limit') is not None else 0) # pyright: ignore
        # NOTE : THIS IS FINE
        llm_response = llm_call(api, prompt)
    except httpx.HTTPStatusError as e:
        log_json({
            'type': "api",
            'status': "error",
            'api_url': api['url'],
            'error_code': e.response.status_code,
            'prev_rate_limit': api.get('rate_limit')
            })
        if e.response.status_code == 429:
            prev_ratelimit = api.get('rate_limit')
            if prev_ratelimit in (None, 0, 1):
                with api['lock']:
                    api['rate_limit'] = 2
            else:
                with api['lock']:
                    api['rate_limit'] = min(api['rate_limit'] ** 2, 120)
        raise e
    else:
        prev_ratelimit = api.get('rate_limit')
        if prev_ratelimit in (None, 0, 1):
            with api['lock']:
                api['rate_limit'] = 0
        else:
            with api['lock']:
                api['rate_limit'] = api['rate_limit'] // 2
                log_json({
                    'type': 'api',
                    'status': 'normal',
                    'api_url': api['url'],
                    'prev_ratelimit': api.get('rate_limit')
                    })
        
    return llm_response


@interruptable(executor_interrupt_queue, global_interrupt_queue)
async def core(
        checkpoint: Callable[[], Coroutine[Any, Any, None]],
        queue: Uqueue[instr_json],
        apis: Sequence[api],
        ) -> None:

    await checkpoint()
    conn = conn_factory()
    conn.autocommit = False

    while True:
        try:
            await checkpoint()
            instr = await queue.get()

            str_instr = " ".join((instr["context"], instr["instruction"]))
            print(str_instr)
            
            for api_sps in apis:
                await checkpoint()
                try:
                    llm_response = llm_call_with_ratelimit(api_sps, str_instr)
                    await checkpoint()
                except httpx.HTTPStatusError:
                    continue
                else:
                    break
            else:
                log_json({
                    'type': 'api',
                    'status': 'error',
                    'api': 'all'
                })
                for _ in range(config['cores_number']):
                    global_interrupt_queue.put("WAIT")
                continue
            await checkpoint()
            log_json({
                'type': 'llm_response',
                'status': 'normal',
                'response': llm_response
            })
            try:
                tool_calls: tool_calls_block = llm_to_json(llm_response)
            except ValueError:
                llm_without_think = re.sub(r'<think>.*?</think>', '', llm_response, re.DOTALL)
                tool_calls = json.loads('[{"tool": "result.write", "args": {"text": ' + f'"{llm_without_think}"' + '}}]')
                log_json({
                    'type': 'llm_response',
                    'status': 'abnormal',
                    'reason': 'did not find any tool calls.',
                    'new_tool_calls_block': tool_calls,
                    'llm_without_think': llm_without_think
                })

            results = []

            metadata_c: _exec_tool_meta_data = {
                    'conn': conn,
                    'master_id': instr['master_addr'],
                    '_embedder_queue': Uqueue[ReferenceTo]()
                    }

            for call in tool_calls:
                await checkpoint()
                try:
                    with conn.transaction():
                        tool_result = execute_tool(call, metadata_c)
                except Exception as e:
                    log_json({
                        'type': 'tool',
                        'status': 'error',
                        'message': str(e),
                        'tool': call,
                        'metadata': metadata_c
                    })
                    prompt = f"""The following tool call failed for the following reason: {call}, {e}
                    Your task is to figure out what went wrong there, and create a working tool call.
                    Here is what it attempted to do "{instr['instruction']}".
                    The following is the tool call format instructions and all the valid tools:
                    """ + "\n".join(HEADERS_REGISTRY.values())
                    llm_output_new = None
                    for api_sps in apis:
                        await checkpoint()
                        try:
                            llm_output_new = llm_call_with_ratelimit(api_sps, prompt)
                        except Exception:
                            continue
                        else:
                            break
                    if llm_output_new is None:
                        log_json({
                            'type': 'api',
                            'status': 'error',
                            'api': 'all'
                        })
                        for _ in range(config['cores_number']):
                            global_interrupt_queue.put("WAIT")
                    new_calls = llm_to_json(llm_output_new)
                    log_json({
                        'type': 'tool_error_recovery',
                        'status': 'normal',
                        'new_llm_output': llm_output_new,
                        'new_tool_calls': new_calls
                    })
                    nresults = []
                    for ncall in new_calls:
                        await checkpoint()
                        try:
                            with conn.transaction():
                                ntool_result = execute_tool(ncall, metadata_c)
                        except Exception as e2:
                            log_json({
                                'type': 'tool_error_recovery',
                                'status': 'error',
                                'recovery_call': ncall,
                                'error': str(e2),
                                'original_call': call,
                                'original_error': str(e)
                            })
                            raise RuntimeError(f"Recovery LLM call failed. Original llm call: {call}, recovery calls {new_calls}, the failed call: {ncall}, error: {e}") from e
                        await checkpoint()
                        nresults.append(ntool_result)
                    results.extend(nresults)
                    continue

                results.append(tool_result)

            result_str = "\n".join([str(r) for r in results])

            await checkpoint()
            conn.execute("""
            SELECT new_result(%s, %s);
                         """, (result_str, instr["result_addr"]))
            conn.commit()

            items = metadata_c['_embedder_queue'].get_all()
            for i in items:
                embedder_queue.put(i)
        
        except Exception as e:
            print(f"CORE THREAD ERROR CAUGHT: {e}, REVERTING TRANSACTION")
            conn.rollback()
            print("RESTART OF CORES NOT YET IMPLEMENTED, please restart the system or implement it")
            raise e from e

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


    apis = []
    for i in config['apis']:
        i['rate_limit'] = 2
        i['lock'] = threading.Lock()
        apis.append(i)

    for _ in range(config["cores_number"]):
        threading.Thread(
                target=core_thread,
                args=(core, executor_queue, apis),
                daemon=True
                ).start()
    print("startup of the executor finished")

    embedder.setup()

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
