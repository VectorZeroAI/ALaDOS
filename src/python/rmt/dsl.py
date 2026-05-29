#!/usr/bin/env python3

from typing import Sequence, TypeAlias, TypedDict

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
    deps: Sequence[str|int]

class RmtNodeIncomplete(RmtNode, total=False):
    pass

ParsedRmtExpression: TypeAlias = list[RmtNode]


def parse(expression: str) -> ParsedRmtExpression:
    """
    The parser function for the RMT expression.
    """
    errors: list[str] = []

    result: ParsedRmtExpression = []

    lines = expression.splitlines()
    for line in lines:
        line = line.strip()
        tokens_raw = line.split(r'->')
        
        tokens_valid = []
        for token in tokens_raw:
            token = token.strip()

            if token == "START" or token == "END":
                continue

            if token.startswith('(') and token.endswith(')'):
                tokens_valid.append(token)

            else:
                errors.append(f"Invalid token {token}, tokens must be encapsulated in parenthesis.")

        for token in tokens_valid:
            item: RmtNodeIncomplete = {}
            for key_val in token.split(','):
                for key, val in key_val.split('='):
                    match key:
                        case 'instruction':
                            pass
                        case 'id':
                            pass
                        case _:
                            errors.append(f"Invalid key found. Key: {key}, key_val pair: {key_val}, token: {token}")

    return errors
