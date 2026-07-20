#!/usr/bin/env python3

from typing import Iterable, Sequence
import tomllib

from ..executor.types import Api


def load_apis(file_path: str) -> Sequence[Api]:
    """ Loads the APIS from the config file """
    with open(file_path, 'rb') as f:
        raw_config = tomllib.load(f)

    apis = []
    for i in raw_config['apis']:
        a = Api(
            url=i['url'],
            key=i['key'],
            model=i['model'],
        )
        if i.get('claude'):
            a.claude = True
        if i.get('max_tokens'):
            a.max_tokens = i['max_tokens']

        apis.append(a)

    return apis

def load_apis_from_text(text: str) -> Sequence[Api]:
    """ Load apis from config text """

    raw_config = tomllib.loads(text)
    
    apis = []
    for i in raw_config['apis']:
        apis.append(Api(
            url=i['url'],
            key=i['key'],
            model=i['model'],
        ))

    return apis
