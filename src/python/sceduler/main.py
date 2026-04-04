#!/usr/bin/env python3

"""
The sceduler subsystem.

Has the function setup(), wich first reconstructs the state in memory from the DB, and then spawns the thread. 

Consists of 1 thread, that listens on the "new_result_inserted" postgres chanell,
Tracks wich tasks are already being executed, and executes all the other tasks. 
"""

def setup():
    
