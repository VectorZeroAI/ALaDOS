#!/usr/bin/env python3
"""
Executables base state file, here all the executables in the base state belong.
"""

from ..types import Executable
from ..registry import register

register(
    Executable(
        "Test Executable",
        "print('Test')",
        "Prints test when executed.",
        "test_tool",
        -1
    )
)

register(
    Executable(
        description="Read Knowledge Item.",
        body="""
        from ALaDOS.lib.Knowledge import read
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        
        id = args.get("id")
        if not id:
            raise ValueError("Id not given.")

        return f"Knowledge Entry at id {id}, content: {asyncio.run(read(id, args["slave_id"]))}."
        """,
        header="""
        args = {
            "id": "knowledge entry id."
        }
        """,
        name="K.read",
        addr=-2
    )
)
