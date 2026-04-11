#!/usr/bin/env python3
import threading
from ..utils.conn_factory import conn_factory

def setup():
    threading.Thread(target=embedder_thread, daemon=True, args=(  )).start()


def embedder_thread():
    conn = conn_factory()
    
