#!/usr/bin/env python3
import sys
import threading

from ..utils.conn_factory import Conn, conn_factory


def add_master(instruction: list[str], conn: Conn):
    conn.execute("""
                 SELECT new_master(
                     p_instruction := (%s)::TEXT
                                  )
                 """, (" ".join(instruction),))

def _webui_thread(port: int):
    from . import webui
    print(f"starting webui at port {port}, at host localhost, e.g. 127.0.0.1")
    webui.webserver.run(port = port)


def start_webui(port: str):
    try:
        port_int = int(port)
    except Exception as e:
        print(f"invalid port supplied. Supplied {port}, error {e}")
        return
    threading.Thread(target=_webui_thread, args=(port_int,), daemon=True).start()

    

def start_console():
    """ The function that starts the server side controll console of alados. """
    conn = conn_factory()
    print("ALaDOS sever side console started.")
    while True:
        try:
            command_str = input("admin > ")
            command = command_str.split(" ")
            match command:
                case "exit", *_:
                    print("exit is not implemented yet. This terminal will remain utilised until alados server is shut down in the current version") # TODO : Fix this
                case "shutdown", *_:
                    print("shutdown initiated")
                    sys.exit(0)
                case "add", "task", *_:
                    print(f"adding task {" ".join(command[2:])} as a master goal") # TODO: use slicing insdead of raw "2"
                    add_master(command[2:], conn)
                case "start", "webui", *_:
                    start_webui(command[2])
                case _:
                    print("undefined command")
        except Exception as e:
            print(f"encoutered error {e}. ignoring because i am a console")
