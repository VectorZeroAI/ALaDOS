#!/usr/bin/env python3
import json
from json_repair import repair_json
from ..executor.types import tool_calls_block
from pydantic import TypeAdapter, ValidationError

def llm_to_json(input_str: str) -> tool_calls_block:
    """ Find the last {...} block, accounting for nested braces """
    last_match = None
    rev = input_str[::-1]
    for i, char in enumerate(rev):
        if char == '}':
            depth = 0
            for j, c in enumerate(rev[i:], i):
                if c == '}':
                    depth += 1
                elif c == '{':
                    depth -= 1
                if depth == 0:
                    # Slice from reversed string, then reverse the slice back
                    last_match = rev[i:j+1][::-1]
                    break
        if last_match is not None:
            break

    if last_match is None:
        raise ValueError("No JSON object found in model output")

    validator = TypeAdapter(tool_calls_block)
    try:
        return validator.validate_json(last_match)
    except ValidationError as e:
        print(f"The following error was encountered during loading of llm toolcalls json: {e}")
        try:
            return validator.validate_json(repair_json(last_match))
        except Exception as e:
            print(f"The following error was encountered during loading of llm toolcalls repaired json: {e}")
            raise NotImplementedError("tool call error recovery not yet implemented", e) from e
            # TODO : RAISE AN ERROR RECOVERY INTERRUPT.
