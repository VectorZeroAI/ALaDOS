#!/usr/bin/env python3

from typing import Callable, Sequence
import tomllib
import httpx
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
    await checkpoint()

    for api_spec in api_specs_sorted:
        await checkpoint()
        time.sleep(api_spec['rate_limited_until'] - time.monotonic())
        await checkpoint()
        try:
            llm_result = llm_call(api_spec, prompt)
            await checkpoint()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                if e.response.headers.get('Retry-After') is float:
                    await checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.monotonic() + e.response.headers.get('Retry-After')
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
                    continue
        else:

            await checkpoint()
            break
    else:

        await checkpoint()
        for _ in config['cores_number']:
            global_interrupt_queue.put('WAIT')
            log_json({
                'type': 'api',
                'status': 'error',
                'api': 'all'
            })
            return None

    return llm_result



def llm_call_with_ratelimit(api: api, prompt: str) -> str:
    """
    The function that encapsulates the entire logic of the llm call with rate limit in itself.
    """

    try:
        time.sleep(api.get('rate_limit') if api.get('rate_limit') is not None else 0) # pyright: ignore
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
