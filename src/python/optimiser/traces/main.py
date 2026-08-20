#!/usr/bin/env python3
"""
The main tracing file, where all the tracing logic should happen. 

This file provides shared resources for surrounding files, which provide per component tracing functions 
that the components import and hook and use.

Also surrounding files should be imported as full modules 
and their hook functions be accessed with dot notation, for simplicity reasons.
"""
from ...utils.conn_factory import conn_factory

conn = conn_factory() # TODO: add retries. 

