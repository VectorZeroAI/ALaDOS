import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import _ExecToolMetaData
from python.utils.conn_factory import conn_factory
from python.executor.execute_tool import tools_manager


TEST_DB_NAME = "alados_test"
os.environ["ALADOS_DB_NAME"] = TEST_DB_NAME


@pytest.fixture
def db():
    conn = conn_factory(TEST_DB_NAME)
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        try:
            conn.execute("ROLLBACK")
        finally:
            conn.close()


@pytest.fixture
def meta(db, monkeypatch):
    """Create a master+slave pair and current execution metadata."""
    master_addr = db.execute_fetchval("SELECT new_master('test master')")
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', 'dummy_slave', NULL, NULL, NULL, NULL, 'general')",
        (master_addr,),
    )

    # Builtin tools require both execution queues and a NATS client in the
    # current _ExecToolMetaData contract. Most unit tests do not actually use
    # the NATS client, so a mock is sufficient.
    return _ExecToolMetaData(
        master_id=master_addr,
        conn=db,
        slave_id=slave_addr,
        context_limit=10000,
        occ_last_change=datetime(2023, 1, 1),
        syscalls_queue=syscalls_queue_dict_per_slave[slave_addr],
        nats=MagicMock(),
    )

    yield meta

    tools_manager.cache.clear()
    tools_manager.cache.update(old_cache)
    tools_manager.conn = old_conn


def unique_name(prefix="test"):
    import uuid
    return f"{prefix}_{uuid.uuid4().hex}"

