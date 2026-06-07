#!/usr/bin/env python3

from typing import TypeAlias, TypedDict
from collections import deque, defaultdict
import uuid

"""
This is the DSL for the rmt.
For DSL specification, see the documentation.
"""

"""
DSL EXAMPLE: 
START -> (task='task_text', id='lol1', scope='general') -> (task='task_text', scope='task', id='anything really') -> (task='task_text') -> (task='task_text') -> (task='task_text') -> (task='task_text', id='stuff') -> END
START -> (task='task_text_3') -> (id='lol1')
"""

from .types import ReturnParsedRmtExpression, ParsedRmtExpression, RmtNodeIncomplete, RmtNode

def parse(expression: str) -> ReturnParsedRmtExpression:
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
                    case 'scope':
                        item['scope'] = val.strip().strip("'")
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

            if item.get('scope') is None:
                item['scope'] = "general"

            intermidiate_result[item['index']] = item
            continue

        for index in range(len(tokens_valid) - 1):
            index = index + 1 + len(global_tokens_valid) - len(tokens_valid) # Corrected index from 0 based to 1 based, to match the counter
            # And shifted to the lovcal tokens indexes area in the global index space.
            # God that is some horrible code
            token_1 = intermidiate_result[index]
            token_2 = intermidiate_result[index + 1]

            token_2['deps'].append(token_1['id'])

    # Validation of the final results block.

    result = [i for i in intermidiate_result.values()]

    if has_cycle(result):
        errors.append("Cycle detected! Dunno where.")

    if errors:
        raise SyntaxError(str(errors))

    dedup: dict[int|str, RmtNode] = {}

    for i in intermidiate_result.values():
        dedup[i['id']] = i

    result = []

    for i in dedup.values():
        result.append(i)

    return_result: ReturnParsedRmtExpression = []

    for i in result:
        return_result.append({'id': i['id'], 'deps': i['deps'], 'instruction': i['instruction'], 'scope': i['scope']})

    return return_result


def has_cycle(nodes: list[RmtNode]) -> bool:
    unique = {node['id']: node for node in nodes}
    graph = {nid: node['deps'] for nid, node in unique.items()}
    
    # Build reverse adjacency: which nodes depend on a given node
    dependents = defaultdict(list)
    for nid, deps in graph.items():
        for dep in deps:
            dependents[dep].append(nid)
    
    # in_degree = number of prerequisites (len(deps))
    in_degree = {nid: len(deps) for nid, deps in graph.items()}
    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    processed = 0
    
    while queue:
        nid = queue.popleft()
        processed += 1
        for dependent in dependents[nid]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    return processed != len(graph)


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

