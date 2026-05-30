#!/usr/bin/env python3

from typing import Sequence, TypeAlias, TypedDict
import uuid

"""
This is the DSL for the rmt.
For DSL specification, see the documentation.
"""

"""
DSL EXAMPLE: 
START -> (task='task_text', id='lol1212123') -> (task='task_text', id='anything really') -> (task='task_text') -> (task='task_text') -> (task='task_text') -> (task='task_text', id='stuff') -> END
"""

class RmtNode(TypedDict):
    instruction: str
    id: str|int
    deps: list[str|int]
    index: int

class RmtNodeIncomplete(RmtNode, total=False):
    pass

ParsedRmtExpression: TypeAlias = list[RmtNode]


def parse(expression: str) -> ParsedRmtExpression:
    """
    The parser function for the RMT expression.
    """

    token_counter = 0

    errors: list[str] = []

    result: ParsedRmtExpression = []
    
    intermidiate_result: dict[int, RmtNodeIncomplete] = {}

    lines = expression.splitlines()
    for line in lines:
        line = line.strip()
        tokens_raw = line.split(r'->')
        
        tokens_valid: list[str] = []
        for token in tokens_raw:
            token = token.strip()

            if token == "START" or token == "END":
                continue

            if token.startswith('(') and token.endswith(')'):
                tokens_valid.append(token)

            else:
                errors.append(f"Invalid token {token}, tokens must be encapsulated in parenthesis.")

        

        for token in tokens_valid:
            token_counter = token_counter + 1
            item: RmtNodeIncomplete = {} # pyright: ignore

            item['index'] = token_counter

            for key_val in token.split(','):
                for key, val in key_val.split('='):

                    if not validate_value(val):
                        errors.append(f"Invalid value: {val}.")
                        continue

                    match key:
                        case 'instruction':
                            item['instruction'] = val.strip().strip("'")
                        case 'id':
                            item['id'] = val.strip().strip("'")
                        case _:
                            errors.append(f"Invalid key found. Key: {key}, key_val pair: {key_val}, token: {token}")
                            continue

            item['id'] = item.get('id', uuid.uuid4())
            if item.get('instruction') is None:
                errors.append(f"invalid object parsed. Object {item}")
                raise SyntaxError(str(errors))

            item['deps'] = item.get('deps', [])

            intermidiate_result[item['index']] = item

        for index in range(len(tokens_valid) - 1)):
            index = index + 1 # Corrected index from 0 based to 1 based, to match the counter
            token_1 = intermidiate_result[index]
            token_2 = intermidiate_result[index + 1]

            token_2['deps'].append(token_1['id'])

    if errors:
        raise SyntaxError(str(errors))

    for i in intermidiate_result.values():
        result.append(i)

    return result

def validate_value(value: str) -> bool:
    """ Validates the value. """
    if not (value.strip().startswith("'") and value.strip().endswith("'")):
        return False
    return True

