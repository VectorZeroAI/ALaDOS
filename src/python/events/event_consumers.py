#!/usr/bin/env python3
from ..utils.conn_factory import Conn
from .types import EventConsumer


def load_event_consumers(conn: Conn) -> list[EventConsumer]:
    """
    This function loads all the Event consumers from the DB for the consumer thread.
    """

    conn.execute("""
                 """)
