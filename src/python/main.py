#!/usr/bin/env python3
"""
Main entrypoint, and the first file to start. 
"""

from pathlib import Path
import asyncio
from .utils.conn_factory import conn_factory

def main():
    """
    The main function that starts everything 
    Connects to the DB, reads the config, starts the executor cores, and starts the user interface. 
    """
    conn = conn_factory()
    curr = conn.cursor()

    main_file = Path(__file__)
    sql_dir = main_file.parent.parent / "sql"

    for i in sorted(sql_dir.glob("*.sql")):
        try:
            curr.execute(i.read_text())
        except Exception as e:
            raise psycopg2.DatabaseError(f"the setup of the db via the sql files failed. reason: {e}") from e
    
if __name__ == "__main__":
    main()
