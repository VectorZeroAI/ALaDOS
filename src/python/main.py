#!/usr/bin/env python3
"""
Main entrypoint, and the first file to start. 
"""

from pathlib import Path
import asyncio
from .utils.conn_factory import conn_factory
import psycopg
from .executor.main import startup as e_startup
from .sceduler.main import setup as s_setup
from .interfaces.alados_console import start_console
from .utils.logger import startup as l_startup, log_json

def main():
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    conn = conn_factory()

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
