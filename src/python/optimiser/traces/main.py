#!/usr/bin/env python3
"""
The main tracing file, where all the tracing logic should happen. 

This file provides shared resources for surrounding files, which provide per component tracing functions 
that the components import and hook and use.
"""
from ...utils.conn_factory import conn_factory

conn = conn_factory() # TODO: add retries. 

