#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from json_repair import repair_json
from ..executor.types import tool_calls_block, tool_call, JsonSerializable
from pydantic import TypeAdapter, ValidationError

def extract_json_block(text: str) -> str:
    """
    Extracts the first complete JSON object or array from the text.
    Returns the JSON string or raises ValueError if not found.
    """
    # Find the first '{' or '['
    start_idx = None
    start_char = None
    for i, ch in enumerate(text):
        if ch in ('{', '['):
            start_idx = i
            start_char = ch
            break
    if start_idx is None:
        raise ValueError("No JSON object or array found in model output")

    # Matching closing character
    end_char = '}' if start_char == '{' else ']'
    depth = 0
    in_string = False
    escape = False
    for i in range(start_idx, len(text)):
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
        if ch == start_char:
            depth += 1
        elif ch == end_char:
            depth -= 1
            if depth == 0:
                return text[start_idx:i+1]
    raise ValueError("Unbalanced JSON brackets")

def llm_to_json(input_str: str) -> tool_calls_block:
    json_str = extract_json_block(input_str)

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
