#!/usr/bin/env python3
"""
Base state.

Base state is basically the system provided knowledge entires, rmts, tools, etc.
The system enforces that they are present at startup,
and the system generally disallows changing them.

It is of course bypassable if you really need to change them,
but you cant delete them, as they will be forced back into existance at next startup.

There is a meta toolkit for defining those in the registry.py file in this directory. 
main.py handles the enforcement at startup time,
and then there is a directory full of files that are the actual definitions. 
"""

from ..utils.conn_factory import conn_factory


def startup() -> None:
    conn = conn_factory()

    print("Base state finished.")
