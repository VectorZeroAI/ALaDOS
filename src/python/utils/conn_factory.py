#!/usr/bin/env python3
import psycopg

def conn_factory() -> psycopg.Connection:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.
    """
    conn = psycopg.connect(
            host="127.0.0.1",
            port=5432,
            dbname="postgres",
            user="u0_a453"
            )
    conn.autocommit = True
    return conn
