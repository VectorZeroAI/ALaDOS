#!/usr/bin/env python3
"""
Main entrypoint, and the first file to start. 
"""

import sys
from pathlib import Path

import psycopg

from .executor.main import startup as e_startup
from .interfaces.alados_console import start_console
from .sceduler.main import setup as s_setup
from .utils.conn_factory import conn_factory_raw
from .utils.logger import startup as l_startup


def main():
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    try:
        conn = conn_factory_raw()
    except psycopg.OperationalError as e:
        print(f"Are you sure you started postgres? \n\n I couldnt connect with this error {e}. \n\n Make sure you actually started postgres.")
        sys.exit(1)

    main_file = Path(__file__)
    sql_dir = main_file.parent.parent / "sql"

    for i in sorted(sql_dir.glob("*.sql")):
        try:
            conn.execute(i.read_text()) # pyright: ignore
            print(f"sql file {i.name} was successfully executed")
        except Exception as e:
            raise psycopg.DatabaseError(f"the setup of the db via the sql files failed. reason: {e}") from e

    e_startup()
    s_setup()
    l_startup()

    print("startup of the server finished.")

    start_console()
    
if __name__ == "__main__":
    main()
