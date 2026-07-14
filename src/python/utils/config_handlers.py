#!/usr/bin/env python3

from typing import Iterable, Sequence
import tomllib

from ..executor.types import Api


def load_apis(file_path: str) -> Sequence[Api]:
    """ Loads the APIS from the config file """
    with open(file_path, 'r') as f:
        raw_config = tomllib.load(f)

    apis = []
    for i in raw_config['apis']:
        apis.append(Api(
            url=i['url'],
            key=i['key'],
            model=i['model'],
        ))

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
