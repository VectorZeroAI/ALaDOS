#!/usr/bin/env python3

from typing import Callable, Sequence
import tomllib
import httpx
import asyncio
import time

from ..executor.types import api
from ..utils.logger import log_json
from ..utils.config_dir_resolver import config_dir_resolver
from ..queue import global_interrupt_queue

config_dir = config_dir_resolver()
config_file = config_dir / "executor.toml"
config = tomllib.loads(config_file.read_text())

def insertion_sort_by_key(arr, key):
    for i in range(1, len(arr)):
        current = arr[i]
        val = current[key]          # the numeric value to compare
        j = i - 1
        # shift larger items one step to the right
        while j >= 0 and arr[j][key] > val:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = current
    return arr



async def api_calls_block(api_specs: Sequence[api], checkpoint: Callable, prompt: str) -> str|None:
    """
    The api calls block. Returns None if no API worked. Returns llm_result str if one API worked.
    """
    await checkpoint()
    api_specs_sorted = insertion_sort_by_key(api_specs, 'rate_limited_until')
    api_specs_sorted = sorted(api_specs_sorted, key=lambda x: x['rate_limited_until'])
    await checkpoint()

    print(f"BEGIN API CALLS BLOCK, APIS SPECS STATUS: {api_specs}")

    for api_spec in api_specs_sorted:
        await checkpoint()
        await asyncio.sleep(max(api_spec['rate_limited_until'] - time.monotonic(), 0))
        await checkpoint()
        try:
            llm_result = llm_call(api_spec, prompt)
            await checkpoint()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                if e.response.headers.get('Retry-After') is not None:
                    try:
                        sleep_seconds = float(e.response.headers.get('Retry-After'))
                    except ValueError:
                        sleep_seconds = 5
                    await checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.monotonic() + min(sleep_seconds, 60)
                        api_spec['consecutive_ratelimits'] += 1
                    continue
                if api_spec['consecutive_ratelimits'] == 0:
                    await checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.monotonic() + 5
                        api_spec['consecutive_ratelimits'] += 1
                    continue
                else:
                    await checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.monotonic() + (5 * api_spec['consecutive_ratelimits'])
                        api_spec['consecutive_ratelimits'] += 1
                    continue
        else:
            with api_spec['lock']:
                api_spec['consecutive_ratelimits'] = 0
                api_spec['rate_limited_until'] = 0
            await checkpoint()
            break
    else:

        await checkpoint()
        for _ in range(config['cores_number']):
            global_interrupt_queue.put('WAIT')
            log_json({
                'type': 'api',
                'status': 'error',
                'api': 'all'
            })

        return None

    return llm_result

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
