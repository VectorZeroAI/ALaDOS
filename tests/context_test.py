#!/usr/bin/env python3
"""
Comprehensive tests for context resolution and item loaders.
"""

import pytest
from python.context.main import resolve_context, resolve_window, resolve_loads, resolve_req_results
from python.context.item_loaders_registry import load_item
from python.context.types import SlaveObj
from .conftest import db, unique_name  # noqa: F401


def insert_knowledge(db, name, content, description):
    addr = db.execute_fetchval("INSERT INTO knowledge (content) VALUES (%s) RETURNING addr", (content,))
    db.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (addr, name))
    db.execute("INSERT INTO vector_ops (addr_k, description, emb) VALUES (%s, %s, %s::vector(768))",
               (addr, description, "[" + ",".join(["0"] * 768) + "]"))
    return addr

def insert_executable(db, name, header, body, description):
    addr = db.execute_fetchval("INSERT INTO executables (header, body) VALUES (%s, %s) RETURNING addr", (header, body))
    db.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (addr, name))
    db.execute("INSERT INTO vector_ops (addr_exe, description, emb) VALUES (%s, %s, %s::vector(768))",
               (addr, description, "[" + ",".join(["0"] * 768) + "]"))
    return addr

def insert_master(db):
    master_addr = db.execute_fetchval("SELECT new_addr()")
    result_addr = db.execute_fetchval("SELECT new_addr()")
    db.execute("INSERT INTO results (addr) VALUES (%s)", (result_addr,))
    db.execute("INSERT INTO masters (addr, instruction, result_addr) VALUES (%s, '', %s)", (master_addr, result_addr))
    return master_addr

def insert_slave(db, master_addr, name, instruction, result_name, requires=None):
    result_addr = db.execute_fetchval("SELECT new_addr()")
    db.execute("INSERT INTO results (addr) VALUES (%s)", (result_addr,))
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, %s, %s, %s, %s, %s, NULL, 'general')",
        (master_addr, instruction, name, requires or [], result_addr, result_name)
    )
    # new_slave already handles slave_req, so no manual insert
    return slave_addr


@pytest.fixture
def scenario(db):
    k_addr = insert_knowledge(db, "TestKnowledge", "K content", "knowledge desc")
    e_addr = insert_executable(db, "TestExec", "def foo()", "body", "exec desc")
    m_addr = insert_master(db)
    s_addr = insert_slave(db, m_addr, "TestSlave", "do something", "my_result")

    db.execute(
        "UPDATE master_context SET window_anchor_knowledge = %s, window_size_l = 3, window_size_r = 3 WHERE addr = %s",
        (k_addr, m_addr)
    )
    db.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (m_addr, k_addr))
    db.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (m_addr, e_addr))

    slave_obj = SlaveObj(addr=s_addr, instruction="do something", master_addr=m_addr,
                         result_name="my_result", scope="general")
    return {"k_addr": k_addr, "e_addr": e_addr, "m_addr": m_addr, "s_addr": s_addr, "slave_obj": slave_obj}


class TestItemLoaders:
    def test_load_knowledge(self, db, scenario):
        result = load_item(scenario["k_addr"], "knowledge", db)
        assert "TestKnowledge" in result

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

    def test_load_master(self, db, scenario):
        result = load_item(scenario["m_addr"], "masters", db)
        assert "master_goal" in result

    def test_load_unknown_type_raises(self, db):
        with pytest.raises(KeyError):
            load_item(1, "nonexistent_type", db)


class TestResolveLoads:
    def test_loaded_items(self, db, scenario):
        loads = resolve_loads(scenario["m_addr"], db)
        assert "TestKnowledge" in loads
        assert "TestExec" in loads

    def test_no_loads(self, db):
        m = insert_master(db)
        loads = resolve_loads(m, db)
        assert "No items are loaded." in loads


class TestResolveWindow:
    def test_knowledge_anchor(self, db, scenario):
        window = resolve_window(scenario["m_addr"], db)
        assert "TestKnowledge" in window

    def test_no_window_placeholder(self, db):
        m = insert_master(db)
        result = resolve_window(m, db)
        assert result == "WINDOW DOES NOT EXIST YET."


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


class TestResolveContext:
    def test_full_context(self, db, scenario):
        ctx = resolve_context(scenario["slave_obj"], db)
        assert "TestKnowledge" in ctx
        assert "TestExec" in ctx

    def test_missing_master_context_row(self, db):
        m = insert_master(db)
        s = insert_slave(db, m, "s", "ins", "r")
        db.execute("DELETE FROM master_context WHERE addr = %s", (m,))
        slave_obj = SlaveObj(addr=s, instruction="ins", master_addr=m, result_name="r", scope="general")
        ctx = resolve_context(slave_obj, db)
        assert "WINDOW DOES NOT EXIST YET." in ctx
