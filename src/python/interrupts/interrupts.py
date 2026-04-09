#!/usr/bin/env python3

from .main import interrupt
import sys

@interrupt("STOP")
def stop_interrupt():
    action_code = input("1 = resume execution, 0 = shutdown")
    if action_code == "1":
        sys.exit(1)
    elif action_code == "0":
        return
