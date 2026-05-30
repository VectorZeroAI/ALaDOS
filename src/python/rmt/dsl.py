#!/usr/bin/env python3

from typing import TypeAlias, TypedDict
import ast
import uuid

"""
This is the DSL for the rmt.
For DSL specification, see the documentation.
"""

"""
DSL EXAMPLE: 
START -> (task='task_text', id='lol1') -> (task='task_text', id='anything really') -> (task='task_text') -> (task='task_text') -> (task='task_text') -> (task='task_text', id='stuff') -> END
START -> (task="task_text_3") -> (id='lol1')
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

    global_tokens_valid: list[str] = []

    errors: list[str] = []

    result: ParsedRmtExpression = []
    
    intermidiate_result: dict[int, RmtNodeIncomplete] = {}

    lines = expression.splitlines()

    lines_valid: list[str] = []
    
    for line in lines:
        if not line:
            continue
        lines_valid.append(line)


    for line in lines_valid:
        line = line.strip()
        tokens_raw = line.split(r'->')

        tokens_valid: list[str] = []
        
        for token in tokens_raw:
            token = token.strip()
            if not token:
                continue

            if token == "START" or token == "END":
                continue

            if token.startswith('(') and token.endswith(')'):
                global_tokens_valid.append(token)
                tokens_valid.append(token)


            else:
                errors.append(f"Invalid token '{token}', tokens must be encapsulated in parenthesis.")

        for token in tokens_valid:
            token_counter = token_counter + 1
            item: RmtNodeIncomplete = {} # pyright: ignore

            item['index'] = token_counter

            token = token.strip("(").strip(")")

            for key_val in token.split(','): # TODO: Figure out a way out of this shit. Propably with a generator or smt.

                print(f"Working on this key_val: {key_val}")
                key, val = key_val.split('=')

                if not validate_value(val):
                    errors.append(f"Invalid value: {val}.")
                    continue

                key = key.strip()
                val = val.strip()

                match key:
                    case 'instruction':
                        item['instruction'] = val.strip().strip("'")
                    case 'id':
                        item['id'] = val.strip().strip("'")
                    case _:
                        errors.append(f"Invalid key found. Key: {key}, key_val pair: {key_val}, token: {token}")
                        continue

            item['id'] = item.get('id', str(uuid.uuid4()))
            if item.get('instruction') is None:
                ref_item_index: int = 0
                for i in intermidiate_result.values():
                    if i['id'] == item['id']:
                        ref_item_index = i['index']
                        break
                else:
                    errors.append(f"invalid object parsed. Object {item}")
                    raise SyntaxError(str(errors))
                
                # At this point, its a referense, as confirmed by the for loop check thingy.

                intermidiate_result[item['index']] = intermidiate_result[ref_item_index]
                continue


            item['deps'] = item.get('deps', [])

            intermidiate_result[item['index']] = item
            continue

        for index in range(len(tokens_valid) - 1):
            index = index + 1 + len(global_tokens_valid) - len(tokens_valid) # Corrected index from 0 based to 1 based, to match the counter
            # And shifted to the lovcal tokens indexes area in the global index space.
            # God that is some horrible code
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

    if value.count("'") > 2:
        return False

    for i in ('(', ')', '#', '<', '>', '='):
        if i in value:
            return False
    return True

