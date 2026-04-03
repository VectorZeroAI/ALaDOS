#!/usr/bin/env python3

import os
from pathlib import Path

def config_dir_resolver():
    match os.name:
        case "posix":
            config_dir = Path("~/.config/ALaDOS").expanduser()
        case "nt":
            config_dir = Path(f"{os.getenv("APPDATA")}/ALaDOS")
        case _:
            raise OSError(f"Unknown OS detected. Detected: {os.name}, expected: nt or posix")
        
    return config_dir
