#!/usr/bin/env python3
import json




def llm_to_json(input_str: str) -> dict:
    """ Find the last {...} block, accounting for nested braces """
    last_match = None
    for i, char in enumerate(input_str):
        if char == '{':
            depth = 0
            for j, c in enumerate(input_str[i:], i):
                if c == '{':
                    depth += 1
                elif c == '}':
                    depth -= 1
                if depth == 0:
                    last_match = input_str[i:j+1]
                    break
    if last_match is None:
        raise ValueError("No JSON object found in model output")
    return json.loads(last_match)
