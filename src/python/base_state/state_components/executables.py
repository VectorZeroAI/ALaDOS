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
