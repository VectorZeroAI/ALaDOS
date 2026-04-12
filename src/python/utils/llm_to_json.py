#!/usr/bin/env python3
from __future__ import annotations

from json_repair import repair_json
from ..executor.types import tool_calls_block
from pydantic import TypeAdapter, ValidationError
import json

def extract_json_block(text: str) -> str:
    """
    Extracts the last complete JSON array or object from the text.
    Returns the JSON string or raises ValueError if not found.
    """
    # Find the last occurrence of ']' or '}'
    end_idx = None
    end_char = None
    for i in range(len(text) - 1, -1, -1):
        if text[i] in (']', '}'):
            end_idx = i
            end_char = text[i]
            break

    if end_idx is None:
        raise ValueError("No JSON object or array found in model output")

    start_char = '[' if end_char == ']' else '{'
    stack = 0
    in_string = False
    escape = False

    # Scan backwards from end_idx to find the matching start_char at depth 0
    for i in range(end_idx, -1, -1):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == end_char:
            stack += 1
        elif ch == start_char:
            stack -= 1
            if stack == 0:
                # Found the matching opening bracket
                candidate = text[i:end_idx+1]
                # Validate that it's parsable JSON
                try:
                    json.loads(candidate)
                    return candidate
                except json.JSONDecodeError:
                    # Invalid JSON, continue searching earlier
                    continue

    raise ValueError("No valid JSON block found")

def llm_to_json(input_str: str) -> tool_calls_block:
    json_str = extract_json_block(input_str)
    print(f"extracted json block = {json_str}")

    validator = TypeAdapter(tool_calls_block)
    validator.rebuild()

    # Attempt direct validation
    try:
        return validator.validate_json(json_str)
    except ValidationError:
        # If it's a single object, wrap it in an array
        if json_str.strip().startswith('{'):
            wrapped = f"[{json_str}]"
            try:
                return validator.validate_json(wrapped)
            except ValidationError:
                pass

        # Try repairing
        repaired = repair_json(json_str)
        try:
            return validator.validate_json(repaired)
        except ValidationError:
            # Try wrapping repaired object
            if repaired.strip().startswith('{'):
                wrapped_repaired = f"[{repaired}]"
                try:
                    return validator.validate_json(wrapped_repaired)
                except ValidationError as e:
                    print(f"Repaired wrapped validation failed: {e}")
                    raise
            raise

    except Exception as e:
        print(f"JSON extraction/validation error: {e}")
        raise NotImplementedError("tool call error recovery not yet implemented", e) from e
