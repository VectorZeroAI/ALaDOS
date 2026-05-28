#!/usr/bin/env python3

"""
This is the DSL for the rmt.
For DSL specification, see the documentation.
"""


"""
START -> (task='task_text', id='lol1212123') -> (task='task_text', id='anything really') -> (task='task_text') -> (task='task_text') -> (task='task_text') -> (task='task_text', id='stuff') -> END
"""

from typing import Sequence, TypeAlias, TypedDict

class RmtNode(TypedDict):
    instruction: str
    id: str|int
    deps: Sequence[str|int]

ParsedRmtExpression: TypeAlias = list[RmtNode]


def parse(expression: str) -> Sequence[str]:
    """
    The parser function for the RMT expression.
    """
    errors: list[str] = []

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
            for key_val in token.split(','):
                pass

    return errors
