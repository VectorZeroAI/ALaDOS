#!/usr/bin/env python3

from .execute_tool import register_tool

# EXAMPLE: 
@register_tool()
def print_to_console(input_str: str) -> None:
    print(input_str)

# TODO : MAKE a friking shitton of tools here, through the whole fucking list
