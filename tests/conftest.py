import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from python.executor.execute_tool import tools_manager
from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import _ExecToolMetaData
from python.utils.conn_factory import conn_factory


TEST_DB_NAME = "alados_test"
os.environ["ALADOS_DB_NAME"] = TEST_DB_NAME


@pytest.fixture
def db():
    """Give each DB test a transaction that is ALWAYS rolled back."""
    conn = conn_factory(TEST_DB_NAME)
    conn.autocommit = False
    conn.rollback()
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


@pytest.fixture
def meta(db):
    """Create execution metadata using the current _ExecToolMetaData contract."""
    master_addr = db.execute_fetchval("SELECT new_master('test master')")
    slave_name = unique_name("dummy_slave")
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', %s, NULL, NULL, NULL, NULL, 'general')",
        (master_addr, slave_name),
    )

    old_cache = tools_manager.cache.copy()
    old_conn = tools_manager.conn
    tools_manager.cache.clear()
    tools_manager.conn = db

    try:
        yield _ExecToolMetaData(
            master_id=master_addr,
            conn=db,
            slave_id=slave_addr,
            context_limit=10000,
            # PostgreSQL returns the timestamp for vector_ops as naive in the
            # current schema.  Tests which explicitly exercise OCC overwrite
            # this with the row's timestamp and therefore expose the production
            # timezone bug instead of manufacturing one in the fixture.
            occ_last_change=datetime.now(),
            syscalls_queue=syscalls_queue_dict_per_slave[slave_addr],
            nats=MagicMock(),
        )
    finally:
        # Do not let temporary executable cache entries or the test connection
        # leak into the next test.
        tools_manager.cache.clear()
        tools_manager.cache.update(old_cache)
        tools_manager.conn = old_conn
        syscalls_queue_dict_per_slave.pop(slave_addr, None)


def unique_name(prefix: str = "test") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"

