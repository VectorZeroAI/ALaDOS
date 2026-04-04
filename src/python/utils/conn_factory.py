#!/usr/bin/env python3
import psycopg2

def conn_factory() -> psycopg2.extensions.connection:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.
    """
    conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="postgres",
            user="u0_a453"
            )
    conn.autocommit = True
    return conn
