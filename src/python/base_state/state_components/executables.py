#!/usr/bin/env python3
"""
Executables base state file, here all the executables in the base state belong.

They basically wrap all the syscalls and maybe give a bit nicer output string,
basically including what the hell the output is of,
so not just the content for example but also where from.

Addresses should be negative,because its system internall tools,
and thus system internal addresses are used, and they are always negative integers.
"""

from ..types import Executable
from ..registry import register

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
        addr=-1
    )
)
