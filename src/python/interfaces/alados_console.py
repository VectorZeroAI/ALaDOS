#!/usr/bin/env python3
import sys
import psycopg
from python.utils.conn_factory import conn_factory

def add_master(instruction: str, conn: psycopg.Connection):
    conn.execute("""
    INSERT INTO masters(instruction) VALUES (%s);
                 """, (instruction,))

def start_console():
    """ The function that starts the server side controll console of alados. """
    conn = conn_factory()
    print("ALaDOS sever side console started.")
    while True:
        command_str = input("admin > ")
        command = command_str.split(" ")
        match command:
            case "exit", _:
                print("exit is not implemented yet. This terminal will remain utilised until alados server is shut down in the current version") # TODO : Fix this
            case "shutdown", _:
                print("shutdown initiated")
                sys.exit(0)
            case "add", "task", _:
                print(f"adding task {command[2]} as a master goal")
                add_master(command[2], conn)
            case _:
                print("undefined command")
