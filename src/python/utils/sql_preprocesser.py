#!/usr/bin/env python3
"""
SQL preprocesser file

The processer is extremely dumb, just providing shorthands for repeated SQL structures.
"""

DEFS: dict[str, str] = {
    "FOLLOW": "ON UPDATE CASCADE ON DELETE CASCADE",
    "ADDR": "REFERENCES addrs(addr) ON UPDATE CASCADE ON DELETE CASCADE"
}

def process(input: str) -> str:
    for key, val in DEFS.items():
        input = input.replace(key, val)

    return input
