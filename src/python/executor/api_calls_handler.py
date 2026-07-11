#!/usr/bin/env python3

from typing import Callable, Sequence
import tomllib
import httpx
import time

from ..executor.exceptions import ContextLimitExceededError
from ..executor.types import Api
from ..utils.logger import log_json
from ..utils.config_dir_resolver import config_dir_resolver
from ..queue import global_interrupt_queue

config_dir = config_dir_resolver()
config_file = config_dir / "executor.toml"
if not config_file.exists():
    raise ValueError(f"CONFIGUATION FILE {config_file} NOT FOUND !!!!")
config = tomllib.loads(config_file.read_text())

def api_calls_block(api_specs: Sequence[Api], checkpoint: Callable, prompt: str) -> str|None:
    """
    The api calls block. Returns None if no API worked. Returns llm_result str if one API worked.
    """
    checkpoint()
    api_specs_sorted = sorted(api_specs, key=lambda x: x['rate_limited_until'])
    checkpoint()
    print(f"API CALLS BLOCK, current ratelimits are: {[r['rate_limited_until'] - time.time() for r in api_specs_sorted]}")

    if len(prompt) > config.get("context_limit", 40000):
        raise ContextLimitExceededError(prompt)

    for api_spec in api_specs_sorted:
        checkpoint()
        time.sleep(max(api_spec['rate_limited_until'] - time.time(), 0))
        checkpoint()
        try:
            llm_result = llm_call(api_spec, prompt)
            print("got llm result!!!")
            checkpoint()
        except httpx.HTTPStatusError as e:
            print(e, e.response, e.response.status_code)
            if e.response.status_code == 429:
                if e.response.headers.get('Retry-After') is not None:
                    try:
                        sleep_seconds = float(e.response.headers.get('Retry-After'))
                    except ValueError:
                        sleep_seconds = 5
                    checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.time() + min(sleep_seconds, 60)
                        api_spec['consecutive_ratelimits'] += 1
                    continue
                if api_spec['consecutive_ratelimits'] == 0:
                    checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.time() + 5
                        api_spec['consecutive_ratelimits'] += 1
                    continue
                else:
                    checkpoint()
                    with api_spec['lock']:
                        api_spec['rate_limited_until'] = time.time() + (5 * api_spec['consecutive_ratelimits'])
                        api_spec['consecutive_ratelimits'] += 1
                    continue
            elif e.response.status_code == 413:
                raise ContextLimitExceededError(prompt)
        else:
            with api_spec['lock']:
                api_spec['consecutive_ratelimits'] = 0
                api_spec['rate_limited_until'] = 0
            checkpoint()
            break
    else:

        checkpoint()
        for _ in range(config['cores_number']):
            global_interrupt_queue.put('WAIT')
            log_json({
                'type': 'api',
                'status': 'error',
                'api': 'all'
            })

        return None

    print("returned llm result")
    return llm_result

def _llm_call_claude(api: Api, prompt: str) -> str:
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


def _llm_call_openai(api: Api, prompt: str) -> str:
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
        try:
            return response.json()["choices"][0]["message"]["content"]
        except KeyError:
            print(response.json())

def llm_call(api: Api, prompt: str) -> str:
    if api.get('claude'):
        return _llm_call_claude(api, prompt)
    else:
        return _llm_call_openai(api, prompt)
