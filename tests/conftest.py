import os

os.environ["ALADOS_DB_NAME"] = "alados_test"

import pytest
from python.utils.conn_factory import conn_factory
from python.executor.types import _ExecToolMetaData
from datetime import datetime


TEST_DB_NAME = "alados_test"



@pytest.fixture
def db():
    conn = conn_factory(TEST_DB_NAME)
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")
        conn.close()


@pytest.fixture
def meta(db):
    """Create a master+slave pair and return _ExecToolMetaData."""
    master_addr = db.execute_fetchval("SELECT new_master('test master')")
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', 'dummy_slave', NULL, NULL, NULL, NULL, 'general')",
        (master_addr,),
    )
    return _ExecToolMetaData(
        master_id=master_addr,
        conn=db,
        slave_id=slave_addr,
        context_limit=10000,
        occ_last_change=datetime(2023, 1, 1),
    )


def unique_name(prefix="test"):
    import random
    return f"{prefix}_{random.randint(10000, 99999)}"
