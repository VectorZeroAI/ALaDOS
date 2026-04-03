#!/usr/bin/env python3
import json
from json_repair import repair_json




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
    try:
        return json.loads(last_match)
    except json.JSONDecodeError as e:
        print(f"The following error was encoutered during loading of llm toolcalls json: {e}")
        try:
            return json.loads(repair_json(last_match))
        except Exception as e:
            print(f"The following error was encoutered during loading of llm toolcalls repaired json: {e}")
            # TODO : RAISE AN ERROR RECOVERY INTERRUPT.
