#!/usr/bin/env python3
"""
Main entrypoint, and the first file to start. 
"""

import sys

import psycopg

from .base_state.main import startup as bs_startup
from .events.main import startup as ev_startup
from .executor.main import startup as e_startup
from .helpers import ensure_schema_applied as db_startup
from .interfaces.alados_console import start_console
from .sceduler.main import setup as s_setup
from .utils.conn_factory import conn_factory_raw
from .utils.logger import startup as l_startup


def main() -> None:
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    try:
        conn = conn_factory_raw()
    except psycopg.OperationalError as e:
        print(f"Are you sure you started postgres? \n\n I couldnt connect with this error {e}. \n\n Make sure you actually started postgres.")
        sys.exit(1)

    db_startup(conn)

    custom_consumers = bs_startup()
    e_startup()
    s_setup()
    l_startup()
    ev_startup(custom_consumers)

    print("startup of the server finished.")

    start_console()
    
if __name__ == "__main__":
    main()
