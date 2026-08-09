#!/usr/bin/env python3
"""
This is the __init__.py file of the state components, it basically imports everything automagically.
"""

import pkgutil
import importlib

# Import all modules in this package
for module_info in pkgutil.iter_modules(__path__):
    importlib.import_module(f'.{module_info.name}', __package__)
