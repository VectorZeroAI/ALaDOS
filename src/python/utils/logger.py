#!/usr/bin/env python3
from python.utils.conn_factory import conn_factory
from python.utils.uqueue import Uqueue
from psycopg.types.json import Jsonb
import threading

_log_conn = conn_factory()

_logger_queue = Uqueue[dict]()

def log_json(content: dict) -> None:
    _logger_queue.put(content)

def _logger_thread() -> None:
    while True:
        _log_curr = _log_conn.cursor()
        sleep(0.3)
        items = _logger_queue.get_all()
        try:
            _log_curr.executemany("""
    INSERT INTO logs(content) VALUES(%s);
                                  """, [(Jsonb(p),) for p in items])
        except Exception as e:
            print(f"LOGGER THREAD ENCOUNTERED THE FOLLOWING ERROR: {e}, retrying")
            try:
                _log_curr.executemany("""
        INSERT INTO logs(content) VALUES(%s);
                                      """, [(Jsonb(p),) for p in items])
            except Exception as e2:
                print(f"retry failed because {e2}. Ignoring that. Here are the logs contents for preservation. {items}")
        _log_curr.close()


def startup():
    threading.Thread(target=_logger_thread, daemon=True).start()
    print("logger thread started successfully")
