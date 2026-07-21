#!/usr/bin/env python3

"""
Test suite for the RMT (Reusable Master Template) subsystem.
Run with: pytest -s test_rmt.py
Requires a running PostgreSQL database with the full ALaDOS schema applied.
Uses the project's Conn type, connected to alados_test, with default TupleRow rows.
"""
from typing import cast

import pytest
import psycopg
import types

from python.rmt.dsl import parse
from python.rmt.main import (
    create_from_serial,
    create_from_master,
    create_from_range,
    delete_node,
    activate_as_master,
    serialize,
)
from python.utils.conn_factory import (
    Conn,
    register_all_the_composite_types,
    _execute_fetchval,
    _execute_many,
)

# -------------------------------------------------------------------
# Test‑specific connection factory (same as conn_factory, but dbname=alados_test)
# -------------------------------------------------------------------

DB_NAME = "alados_test"
DB_HOST = "/data/data/com.termux/files/usr/tmp"

def conn_factory_test() -> Conn:
    """Create a Conn connected to the test DB, with autocommit off."""
    conn_raw = psycopg.connect(host=DB_HOST, dbname=DB_NAME)
    conn_raw.autocommit = False          # we use explicit transactions
    conn = register_all_the_composite_types(conn_raw)
    conn = cast(Conn, conn)
    conn.execute_fetchval = types.MethodType(_execute_fetchval, conn)
    conn.executemany = types.MethodType(_execute_many, conn)

    return conn


@pytest.fixture(scope="function")
def clean_db():
    """Provide a clean test database inside a transaction that is rolled back."""
    conn = conn_factory_test()
    # Clean up tables (order matters for FK constraints)
    tables = [
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves", "reusable_master_templates",
        "slaves", "masters", "results", "names", "vector_ops",
        "executables", "knowledge", "logs", "addrs",
        "cronjob_once", "cronjob_loop",
    ]
    with conn.transaction():
        for t in tables:
            try:
                conn.execute(f"DELETE FROM {t} CASCADE") # pyright: ignore
            except Exception:
                pass
        # Reset sequences
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")
        yield conn

# -------------------------------------------------------------------
# DSL Parser Tests (no DB)
# -------------------------------------------------------------------
class TestDSLParser:
    def test_simple_linear(self):
        expr = "START -> (id='1', instruction='do A') -> (id='2', instruction='do B') -> END"
        result = parse(expr)
        assert len(result) == 2
        assert result[0].id == "1"
        assert result[0].instruction == "do A"
        assert result[0].deps == []
        assert result[1].id == "2"
        assert result[1].instruction == "do B"
        assert result[1].deps == ["1"]

    def test_reference(self):
        expr = """
        START -> (id='1', instruction='first') -> (id='2', instruction='second')
        START -> (id='3', instruction='third') -> (id='2')
        """
        result = parse(expr)
        assert len(result) == 3
        ids = {node.id for node in result}
        assert len(ids) == 3
        node = result[0]
        assert node.instruction == "first"
        assert node.deps == []

    def test_multiline_branching(self):
        expr = """
            START -> (id='1', instruction='root') -> (id='2', instruction='left') -> END
            START -> (id='1') -> (id='3', instruction='right') -> END
               """
        result = parse(expr)
        assert len(result) == 3
        node1 = next(n for n in result if n.id == "1")
        assert node1.deps == []
        node2 = next(n for n in result if n.id == "2")
        assert node2.deps == ["1"]
        node3 = next(n for n in result if n.id == "3")
        assert node3.deps == ["1"]

    def test_missing_instruction(self):
        expr = "START -> (id='1') -> (id='2') -> END"
        with pytest.raises(SyntaxError):
            parse(expr)

    def test_invalid_value(self):
        expr = "START -> (id='1', instruction=unquoted) -> END"
        with pytest.raises(SyntaxError):
            parse(expr)

    def test_cycle_detection(self):
        # DSL can't express cycles, placeholder
        pass

# -------------------------------------------------------------------
# Serialize / Round-trip Tests
# -------------------------------------------------------------------
class TestSerializationRoundTrip:
    def test_roundtrip_linear(self, clean_db):
        dsl = "START -> (id='1', instruction='task1') -> (id='2', instruction='task2') -> END"
        addr = create_from_serial(dsl, name="test_rmt", conn=clean_db)
        print(f"TEST: roundtrip linear: at addr {addr}, DSL = {dsl}")
        serialized = serialize(addr, conn=clean_db)
        print(f"serialized = {serialized}")
        reparsed = parse(serialized)
        print(f"reparsed = {reparsed}")
        assert len(reparsed) == 2
        instructions = {n.instruction for n in reparsed}
        assert instructions == {"task1", "task2"}
        n1 = next(n for n in reparsed if n.instruction == "task1")
        n2 = next(n for n in reparsed if n.instruction == "task2")
        assert n1.deps == []
        assert n2.deps == [n1.id]

# -------------------------------------------------------------------
# create_from_serial Tests
# -------------------------------------------------------------------
class TestCreateFromSerial:
    def test_create_linear(self, clean_db):
        dsl = "START -> (id='1', instruction='do a') -> (id='2', instruction='do b') -> END"
        addr = create_from_serial(dsl, conn=clean_db)
        # Verify reusable_master_templates entry exists (TupleRow)
        row = clean_db.execute(
            "SELECT addr FROM reusable_master_templates WHERE addr = %s", [addr]
        ).fetchone()
        assert row is not None
        # Fetch slaves (instruction, deps) as TupleRows
        slaves = clean_db.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction", [addr]
        ).fetchall()
        assert len(slaves) == 2
        # Each row: (instruction, deps)
        s1 = next(s for s in slaves if s[0] == "do a")
        s2 = next(s for s in slaves if s[0] == "do b")
        # deps may be None or list
        assert s1[1] is None or len(s1[1]) == 0
        assert len(s2[1]) == 1
        # Need s1's addr – we didn't fetch it. We'll do another query to get addrs
        addrs = clean_db.execute(
            "SELECT instruction, addr FROM rmt_slaves WHERE template_addr = %s", [addr]
        ).fetchall()
        a_addr = next(r[1] for r in addrs if r[0] == "do a")
        assert s2[1][0] == a_addr

    def test_create_with_reference(self, clean_db):
        dsl = "START -> (id='1', instruction='task') -> (id='1') -> END"
        with pytest.raises(SyntaxError):
            create_from_serial(dsl, conn=clean_db)

    def test_create_with_name(self, clean_db):
        dsl = "START -> (id='1', instruction='run') -> END"
        addr = create_from_serial(dsl, name="cool_template", conn=clean_db)
        row = clean_db.execute(
            "SELECT name FROM names WHERE addr = %s", [addr]
        ).fetchone()
        assert row[0] == "cool_template"

# -------------------------------------------------------------------
# create_from_master Tests
# -------------------------------------------------------------------
class TestCreateFromMaster:
    def test_basic_master(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('top task', NULL, NULL, 'master_result')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT resolve_name('master_result'))"
        )
        conn.execute("""
            SELECT new_slave(%s, 'step 1', 'slave1', NULL, NULL, 'r1', NULL, 'general')
        """, [master_addr])
        r1_addr = conn.execute_fetchval("SELECT resolve_name('r1')")
        conn.execute("""
            SELECT new_slave(%s, 'step 2', 'slave2', ARRAY[%s], NULL, 'r2', NULL, 'general')
        """, [master_addr, r1_addr])
        rmt_addr = create_from_master(master_addr, name="from_master", conn=clean_db)
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction",
            [rmt_addr]
        ).fetchall()
        assert len(slaves) == 2
        s1 = next(s for s in slaves if s[0] == "step 1")
        s2 = next(s for s in slaves if s[0] == "step 2")
        assert s1[1] is None or len(s1[1]) == 0
        assert len(s2[1]) == 1
        # Get addr of step1 to verify dependency
        a_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'step 1'", [rmt_addr]
        )
        assert s2[1][0] == a_addr

    def test_planner_removal(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('complex task')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        )
        conn.execute("""
            SELECT new_slave(%s, 'plan stuff', 'planner_123', NULL, NULL, NULL, NULL, 'task')
        """, [master_addr])
        conn.execute("""
            SELECT new_slave(%s, 'execute stuff', 'worker', NULL, NULL, NULL, NULL, 'general')
        """, [master_addr])
        rmt_addr = create_from_master(master_addr, conn=clean_db)   # no name
        slaves = conn.execute(
            "SELECT instruction FROM rmt_slaves WHERE template_addr = %s", [rmt_addr]
        ).fetchall()
        instructions = [s[0] for s in slaves]
        assert "plan stuff" not in instructions
        assert "execute stuff" in instructions

# -------------------------------------------------------------------
# create_from_range Tests
# -------------------------------------------------------------------
class TestCreateFromRange:
    def test_basic_range(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('range test', NULL, NULL, 'm_res')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT resolve_name('m_res'))"
        )
        conn.execute("SELECT new_slave(%s, 'A', 'sA', NULL, NULL, 'rA')", [master_addr])
        rA = conn.execute_fetchval("SELECT resolve_name('rA')")
        conn.execute("SELECT new_slave(%s, 'B', 'sB', ARRAY[%s], NULL, 'rB')", [master_addr, rA])
        rB = conn.execute_fetchval("SELECT resolve_name('rB')")
        conn.execute("SELECT new_slave(%s, 'C', 'sC', ARRAY[%s], NULL, 'rC')", [master_addr, rB])
        sA_addr = conn.execute_fetchval("SELECT resolve_name('sA')")
        sC_addr = conn.execute_fetchval("SELECT resolve_name('sC')")
        print(conn.execute_fetchval("SELECT current_database();"))
        rmt_addr = create_from_range(start_node_id=sA_addr, conn=clean_db, end_node_id=sC_addr, name="range_test")
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction",
            [rmt_addr]
        ).fetchall()
        assert len(slaves) == 3
        a = next(s for s in slaves if s[0] == "A")
        b = next(s for s in slaves if s[0] == "B")
        c = next(s for s in slaves if s[0] == "C")
        # Get addrs
        a_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'A'", [rmt_addr]
        )
        b_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [rmt_addr]
        )
        assert b[1] == [a_addr]
        assert c[1] == [b_addr]

    def test_range_with_no_path(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('isolated')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        )
        conn.execute("SELECT new_slave(%s, 'X', 'sX', NULL, NULL, 'rX')", [master_addr])
        conn.execute("SELECT new_slave(%s, 'Y', 'sY', NULL, NULL, 'rY')", [master_addr])
        sX = conn.execute_fetchval("SELECT resolve_name('sX')")
        sY = conn.execute_fetchval("SELECT resolve_name('sY')")
        with pytest.raises(Exception):
            create_from_range(sX, clean_db, sY)

# -------------------------------------------------------------------
# delete_node Tests
# -------------------------------------------------------------------
class TestDeleteNode:
    def test_delete_without_concatenation(self, clean_db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=clean_db)
        conn = clean_db
        node2 = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [addr]
        )
        delete_node(node2, concatenate=False, conn=clean_db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s", [addr]
        ).fetchall()
        instructions = {r[0] for r in remaining}
        assert instructions == {"A", "C"}
        nodeC = next(r for r in remaining if r[0] == "C")
        assert nodeC[1] is None or len(nodeC[1]) == 0

    def test_delete_with_concatenation(self, clean_db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=clean_db)
        conn = clean_db
        nodeB_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [addr]
        )
        nodeA_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'A'", [addr]
        )
        delete_node(nodeB_addr, concatenate=True, conn=clean_db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s", [addr]
        ).fetchall()
        nodeC = next(r for r in remaining if r[0] == "C")
        assert nodeC[1] == [nodeA_addr]

# -------------------------------------------------------------------
# activate_as_master Tests
# -------------------------------------------------------------------
class TestActivateAsMaster:
    def test_activation_basic(self, clean_db):
        dsl = "START -> (id='1', instruction='step1') -> (id='2', instruction='step2') -> END"
        rmt_addr = create_from_serial(dsl, name="basic_template", conn=clean_db)
        activate_as_master(rmt_addr, inputs={}, conn=clean_db)
        conn = clean_db
        master = conn.execute(
            "SELECT * FROM masters WHERE instruction = 'NONE'"
        ).fetchone()
        assert master is not None
        slaves = conn.execute(
            "SELECT instruction FROM slaves WHERE master_addr = %s ORDER BY instruction",
            [master[0]]   # master.addr is first column
        ).fetchall()
        assert len(slaves) == 2
        assert slaves[0][0] == "step1"
        assert slaves[1][0] == "step2"

    def test_activation_with_placeholders(self, clean_db):
        dsl = "START -> (id='1', instruction='Add \"CODE ${{color}}\" to the master result') -> END"
        rmt_addr = create_from_serial(dsl, conn=clean_db)
        activate_as_master(rmt_addr, inputs={"color": "GREEN"}, conn=clean_db)
        conn = clean_db
        slave = conn.execute(
            "SELECT instruction FROM slaves WHERE master_addr = (SELECT addr FROM masters WHERE instruction = 'NONE' LIMIT 1)"
        ).fetchone()
        assert slave is not None
        assert "GREEN" in slave[0]
        assert "${{color}}" not in slave[0]

    def test_activation_with_external_deps(self, clean_db):
        conn = clean_db
        conn.execute("INSERT INTO results (addr, ready, content_str) VALUES (new_addr(), TRUE, 'pre-existing')")
        ext_result = conn.execute_fetchval("SELECT currval('global_next_id')")
        dsl = "START -> (id='1', instruction='use external') -> END"
        rmt_addr = create_from_serial(dsl, conn=clean_db)
        activate_as_master(rmt_addr, depends_on=[ext_result], conn=clean_db)
        master = conn.execute(
            "SELECT m.addr FROM masters m JOIN master_req mr ON m.addr = mr.master_addr WHERE mr.req_addr = %s",
            [ext_result]
        ).fetchone()
        assert master is not None

# -------------------------------------------------------------------
# Other tests
# -------------------------------------------------------------------
def test_insert_node_not_implemented():
    from python.rmt.main import insert_node
    assert callable(insert_node)

def test_parse_empty_lines():
    expr = "\nSTART -> (id='1', instruction='hi') -> END\n\n"
    result = parse(expr)
    assert len(result) == 1

def test_parse_scope():
    expr = "START -> (id='1', instruction='do', scope='task') -> END"
    result = parse(expr)
    assert result[0].scope == "task"
