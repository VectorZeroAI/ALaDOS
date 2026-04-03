#!/usr/bin/env python3
import json
from json_repair import repair_json
from ..executor.types import tool_calls_block
from pydantic import TypeAdapter, ValidationError

def llm_to_json(input_str: str) -> tool_calls_block:
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

    validator = TypeAdapter(tool_calls_block)
    try:
        return validator.validate_json(last_match)
    except json.JSONDecodeError as e:
        print(f"The following error was encountered during loading of llm toolcalls json: {e}")
        try:
            return validator.validate_json(repair_json(last_match))
        except Exception as e:
            print(f"The following error was encoutered during loading of llm toolcalls repaired json: {e}")
            raise NotImplementedError("tool call error recovery not yet implemented", e) from e
            # TODO : RAISE AN ERROR RECOVERY INTERRUPT.
    except ValidationError as e:
        print(f"The following error was encountered during loading of llm toolcalls json: {e}")
        raise NotImplementedError("tool call error recovery not yet implemented", e) from e
