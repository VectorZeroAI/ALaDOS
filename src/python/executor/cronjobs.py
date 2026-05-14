#!/usr/bin/env python3
from ..utils.conn_factory import conn_factory
import threading

def setup():
    conn = conn_factory()
    


def cronjob_executor():
    conn = conn_factory()
    conn.execute("LISTEN cronjob_changes")
