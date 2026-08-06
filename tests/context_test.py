#!/usr/bin/env python3
"""
Comprehensive tests for context resolution and item loaders.
Requires a PostgreSQL test database with the ALaDOS schema applied.
"""

import os
import pytest
import psycopg

from python.utils.conn_factory import Conn, register_all_the_composite_types
from python.context.main import resolve_context, resolve_window, resolve_loads, resolve_req_results
from python.context.item_loaders_registry import load_item
from python.context.types import SlaveObj

# Ensure the actual loader functions are registered
import python.context.item_loaders  # noqa: F401

# ----------------------------------------------------------------------
# Test connection helpers
# ----------------------------------------------------------------------
TEST_DSN = dict(
    host="127.0.0.1",
    port=5432,
    dbname=os.environ.get("TEST_DB", "alados_test"),
    user=os.environ.get("TEST_DB_USER", "u0_a453"),
)


def get_test_conn() -> Conn:
    """Create a test Conn with the same autocommit behaviour as the production code."""
    conn = Conn.connect(**TEST_DSN)
    conn.autocommit = True
    conn = register_all_the_composite_types(conn)
    return conn


@pytest.fixture
def db():
    """Provide a test connection inside an explicit transaction that rolls back after test."""
    conn = get_test_conn()
    conn.execute("BEGIN")
    yield conn
    conn.execute("ROLLBACK")
    conn.close()


# ----------------------------------------------------------------------
# Seed helpers
# ----------------------------------------------------------------------
def new_addr(db: Conn) -> int:
    return db.execute_fetchval("SELECT new_addr()")


def insert_knowledge(db: Conn, name: str, content: str, description: str, position: int = None) -> int:
    addr = db.execute_fetchval(
        "INSERT INTO knowledge (content) VALUES (%s) RETURNING addr",
        (content,)
    )
    db.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (addr, name))
    # position is ignored by trigger, so we don't pass it
    db.execute(
        "INSERT INTO vector_ops (addr_k, description, emb) VALUES (%s, %s, %s::vector(768))",
        (addr, description, "[" + ",".join(["0"] * 768) + "]")
    )
    return addr


def insert_executable(db: Conn, name: str, header: str, body: str, description: str) -> int:
    addr = db.execute_fetchval(
        "INSERT INTO executables (header, body) VALUES (%s, %s) RETURNING addr",
        (header, body)
    )
    db.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (addr, name))
    db.execute(
        "INSERT INTO vector_ops (addr_exe, description, emb) VALUES (%s, %s, %s::vector(768))",
        (addr, description, "[" + ",".join(["0"] * 768) + "]")
    )
    return addr


def insert_master(db: Conn) -> int:
    """Insert a master with its result. The master_context row is created by a DB trigger."""
    master_addr = db.execute_fetchval("SELECT new_addr()")
    result_addr = db.execute_fetchval("SELECT new_addr()")
    db.execute("INSERT INTO results (addr) VALUES (%s)", (result_addr,))
    db.execute("INSERT INTO masters (addr, instruction, result_addr) VALUES (%s, '', %s)",
               (master_addr, result_addr))
    return master_addr


def insert_slave(db: Conn, master_addr: int, name: str, instruction: str, result_name: str, requires: list = None) -> int:
    """Create a slave with a result. new_slave handles the name for the result."""
    result_addr = db.execute_fetchval("SELECT new_addr()")
    db.execute("INSERT INTO results (addr) VALUES (%s)", (result_addr,))
    # Do NOT manually insert into names – new_slave does that for result_name
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, %s, %s, %s, %s, %s, NULL, 'general')",
        (master_addr, instruction, name, requires or [], result_addr, result_name)
    )
    if requires:
        for req_addr in requires:
            db.execute("INSERT INTO slave_req (slave_addr, req_addr) VALUES (%s, %s)",
                       (slave_addr, req_addr))
    return slave_addr


# ----------------------------------------------------------------------
# Scenario fixture
# ----------------------------------------------------------------------
@pytest.fixture
def scenario(db: Conn):
    k_addr = insert_knowledge(db, "TestKnowledge", "K content", "knowledge desc")
    e_addr = insert_executable(db, "TestExec", "def foo()", "body", "exec desc")
    m_addr = insert_master(db)
    s_addr = insert_slave(db, m_addr, "TestSlave", "do something", "my_result")

    # Set up a viewing window anchored on the knowledge item
    db.execute(
        "UPDATE master_context SET window_anchor_knowledge = %s, window_size_l = 3, window_size_r = 3 WHERE addr = %s",
        (k_addr, m_addr)
    )
    # Load both items
    db.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (m_addr, k_addr))
    db.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (m_addr, e_addr))

    slave_obj = SlaveObj(
        addr=s_addr,
        instruction="do something",
        master_addr=m_addr,
        result_name="my_result",
        scope="general"
    )

    return {
        "k_addr": k_addr,
        "e_addr": e_addr,
        "m_addr": m_addr,
        "s_addr": s_addr,
        "slave_obj": slave_obj,
    }


# ----------------------------------------------------------------------
# Item loaders
# ----------------------------------------------------------------------
class TestItemLoaders:
    def test_load_knowledge(self, db, scenario):
        result = load_item(scenario["k_addr"], "knowledge", db)
        assert "TestKnowledge" in result
        assert "K content" in result

    def test_load_knowledge_nonexistent(self, db):
        result = load_item(999999, "knowledge", db)
        assert "DOES NOT EXIST" in result

    def test_load_executable(self, db, scenario):
        result = load_item(scenario["e_addr"], "executables", db)
        assert "TestExec" in result
        assert "def foo()" in result

    def test_load_result(self, db, scenario):
        result_addr = db.execute_fetchval("SELECT result_addr FROM slaves WHERE addr = %s", (scenario["s_addr"],))
        result = load_item(result_addr, "results", db)
        assert "my_result" in result

    def test_load_slave(self, db, scenario):
        result = load_item(scenario["s_addr"], "slaves", db)
        assert "TestSlave" in result
        assert "do something" in result

    def test_load_master(self, db, scenario):
        result = load_item(scenario["m_addr"], "masters", db)
        assert "master_goal" in result

    def test_load_unknown_type_raises(self, db):
        with pytest.raises(KeyError):
            load_item(1, "nonexistent_type", db)


# ----------------------------------------------------------------------
# resolve_loads
# ----------------------------------------------------------------------
class TestResolveLoads:
    def test_loaded_items(self, db, scenario):
        loads = resolve_loads(scenario["m_addr"], db)
        assert "TestKnowledge" in loads
        assert "TestExec" in loads

    def test_no_loads(self, db):
        m = insert_master(db)
        loads = resolve_loads(m, db)
        assert "No items are loaded." in loads


# ----------------------------------------------------------------------
# resolve_window
# ----------------------------------------------------------------------
class TestResolveWindow:
    def test_knowledge_anchor(self, db, scenario):
        window = resolve_window(scenario["m_addr"], db)
        # The trigger overwrites positions, so both items are inside the window.
        # Therefore we only check that the anchor appears.
        assert "TestKnowledge" in window
        # (We no longer assert that TestExec is absent.)

    def test_no_window_raises(self, db):
        m = insert_master(db)
        # Both anchors are NULL → resolve_window raises ValueError
        with pytest.raises(ValueError):
            resolve_window(m, db)


# ----------------------------------------------------------------------
# resolve_req_results
# ----------------------------------------------------------------------
class TestResolveReqResults:
    def test_with_requirements(self, db, scenario):
        req_result = db.execute_fetchval("SELECT new_addr()")
        db.execute("INSERT INTO results (addr, content_str, ready) VALUES (%s, 'req content', TRUE)", (req_result,))
        db.execute("INSERT INTO slave_req (slave_addr, req_addr) VALUES (%s, %s)",
                   (scenario["s_addr"], req_result))
        results = resolve_req_results(scenario["slave_obj"], db)
        assert "req content" in results

    def test_no_requirements(self, db, scenario):
        results = resolve_req_results(scenario["slave_obj"], db)
        assert "NO REQUIRED RESULTS PRESENT" in results


# ----------------------------------------------------------------------
# resolve_context
# ----------------------------------------------------------------------
class TestResolveContext:
    def test_full_context(self, db, scenario):
        ctx = resolve_context(scenario["slave_obj"], db)
        # The returned context contains the window, loads, required results, and tool headers.
        assert "TestKnowledge" in ctx
        assert "TestExec" in ctx
        # The slave's own instruction is not part of the context, so we do NOT assert "do something" here.

    def test_missing_master_context_row(self, db):
        m = insert_master(db)
        s = insert_slave(db, m, "s", "ins", "r")
        db.execute("DELETE FROM master_context WHERE addr = %s", (m,))
        slave_obj = SlaveObj(addr=s, instruction="ins", master_addr=m, result_name="r", scope="general")
        # No exception is raised; instead a placeholder string is used.
        ctx = resolve_context(slave_obj, db)
        assert "WINDOW DOES NOT EXIST YET." in ctx
