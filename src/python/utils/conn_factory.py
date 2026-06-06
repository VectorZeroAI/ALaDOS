#!/usr/bin/env python3
import psycopg
from psycopg.types import composite


def conn_factory() -> psycopg.Connection:
    """
    The factory function for connecting to the database.
    Credentials are hardcoded, because the application sets the DB up internally,
    and there is no user API available for changing it.
    """
    conn = psycopg.connect(
            host="127.0.0.1",
            port=5432,
            dbname="alados"
            )
    conn.autocommit = True

    conn = register_all_the_composite_types(conn)

    return conn


def register_all_the_composite_types(conn: psycopg.Connection) -> psycopg.Connection:


    rmt_node_info = composite.CompositeInfo.fetch(conn, "rmt_node")
    assert rmt_node_info is not None
    composite.register_composite(rmt_node_info)
    return conn
