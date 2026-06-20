#!/usr/bin/env python3

from types import prepare_class
from typing import Any, Generator, TypeAlias, TypedDict
from collections import deque, defaultdict
import uuid

from python.utils.conn_factory import conn_factory

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



def serialise(addr: int) -> str:
    """
    Serialises an reusable master template into its DSL form.
    """
    conn = conn_factory()

    name = conn.execute("""
    SELECT name FROM names WHERE addr = %s
                 """, (addr, )).fetchone()
    
    if not name:
        name = "No Name"
    else:
        name = name[0]

    steps_fetch = conn.execute("""
    SELECT addr, instruction, scope, deps FROM rmt_slaves WHERE template_addr = %s
                               """, (addr,)).fetchall()

    steps_strings = [f"(id='{i[0]}', instruction='{i[1]}', scope='{i[2]}')" for i in steps_fetch]

    steps: list[dict[str, Any]] = [{"str": st_str} for st_str in steps_strings] 

    for i in range(len(steps)):
        steps[i]["deps"] = steps_fetch[i][3]
        steps[i]["addr"] = steps_fetch[i][0]
        steps[i]["seen"] = False
        steps[i]["dupl"] = False


    result: list[list[dict]] = []

    graph: dict[int, list[int]] = {}


    for step in steps:
        graph.setdefault(step['addr'], [])
        graph[step['addr']] = step.get('deps', [])

    flags: dict[int, bool] = {}

    def recursive_worker(path: list[dict], next_node: dict) -> None:
        """
        Walks the graph recursivly and populates the result with results. 
        """

        current_node = next_node
        out_deg = len(graph[next_node['addr']])
        in_deg = len(next_node['deps'])

        flag_moved_on = False

        next_node_addr = graph[current_node['addr']][-1]
        next_node = steps[next_node_addr]
        
        if in_deg > 1:
            match flags[current_node['addr']]:
                case True:
                    if not flag_moved_on:

                        flag_moved_on = True
                        recursive_worker(path, next_node)
                case False:

                    flags[current_node['addr']] = True
                    result.append(path)
                    return


        if out_deg > 1:
            for next_node_addr in graph[next_node['addr']][:-1]:

                next_node = steps[next_node_addr]
                recursive_worker([current_node], next_node)

            if not flag_moved_on:

                flag_moved_on = True
                recursive_worker(path, next_node)
        
        if in_deg == 1 and out_deg == 1:

            recursive_worker(path, next_node)
        
        if out_deg == 0:
            result.append(path)
            return

    def steps_where_no_deps() -> Generator:
        for i in steps.values():
            if len(i['deps']) == 0:
                yield i

    for i in steps_where_no_deps():
        recursive_worker([], i)

#     result: list[list[dict]] = []
#     previous: list[dict] = []
#     next: list[dict] = []
#
#     flag_done = False
# 
#     # First pass, e.g. those with no deps.
# 
#     for st in steps:
#         if st['deps'] is None:
#             result.append([st])
#             previous.append(st)
#             st["seen"] = True
# 
#     for st in previous:
#         steps.remove(st)
# 
# 
#     # Loop recursive resolution
#     while not flag_done:
#         next = []
#         visited = []
# 
#         for i, pr_st in enumerate(previous):
# 
#             if pr_st in visited:
#                 continue
#             else:
#                 visited.append(pr_st)
# 
#             for st in steps:
# 
#                 if pr_st['addr'] in st['deps']:
#                     if not pr_st['seen']:
#                         next.append(st)
#                         result[i].append(st) # Into line i, where pr_st is, of the result append st.
#                         st['seen'] = True
#                     else:
#                         result.append([pr_st, st])
#                         pr_st['dupl'] = True
# 
#         for line in result:
#             for step in line:
#                 try:
#                     steps.remove(step)
#                 except ValueError:
#                     pass
# 
# 
#         previous = next
# 
#         if len(steps) < 1:
#             flag_done = True

        
        

    
    """
    At this point, the data structure is as following: 
    results list, wich holds per line lists of the nodes. 

    So, we just have to take the per line stuff, and then just ' -> '.join(line),
    but we have to figure out referenses. 

    To figure out referenses, we use a list called "visited" and just check if the node is in visited before continuing. 
    If it is, we replace it with a referense, e.g. (id="the_id")
    """

    visited = []

    result_str: list[str] = []

    for _ in range(len(result)):
        result_str.append("")

    for i, line in enumerate(result):
        for node in line:
            
            if node in visited:
                result_str[i] = result_str[i] + f" -> (id='{node['addr']}')"
            else:
                result_str[i] = result_str[i] + f" -> {node['str']}"
                visited.append(node)

    for line in result_str:
        if line.count('(') < 2:
            line = "START -> " + line

    return "\n".join(result_str)

