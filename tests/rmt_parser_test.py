#!/usr/bin/env python3

"""
Test suite for the RMT (Reusable Master Template) subsystem.
Run with: pytest -s test_rmt.py
Requires a running PostgreSQL database with the full ALaDOS schema applied.
All RMT functions (including serialize) accept an optional `conn` parameter.
"""
import pytest
import psycopg
from psycopg.rows import dict_row

# -------------------------------------------------------------------
# Import project modules
# -------------------------------------------------------------------
from python.rmt.dsl import parse
from python.rmt.main import (
    create_from_serial,
    create_from_master,
    create_from_range,
    delete_node,
    activate_as_master,
    serialize,
)

# -------------------------------------------------------------------
# DB helpers for tests
# -------------------------------------------------------------------

DB_NAME = "alados_test"
DB_CONN_STRING = f"dbname={DB_NAME}"

@pytest.fixture(scope="function")
def db_conn():
    """Return a connection wrapped with dict_row, inside a transaction that is rolled back after the test."""
    conn = psycopg.connect(DB_CONN_STRING, row_factory=dict_row)
    conn.autocommit = False
    with conn.transaction():
        yield conn

@pytest.fixture(scope="function")
def clean_db(db_conn):
    """Truncate all relevant tables and reset sequences before each test (still inside the transaction)."""
    tables = [
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves", "reusable_master_templates",
        "slaves", "masters", "results", "names", "vector_ops",
        "executables", "knowledge", "logs", "ownership", "addrs",
        "cronjob_once", "cronjob_loop",
    ]
    for t in tables:
        try:
            db_conn.execute(f"DELETE FROM {t} CASCADE")
        except Exception:
            pass
    # Reset sequences
    db_conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
    db_conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
    db_conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")
    return db_conn

# -------------------------------------------------------------------
# DSL Parser Tests (no DB needed)
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
        serialized = serialize(addr, conn=clean_db)   # <-- serialize now takes conn
        reparsed = parse(serialized)
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
        row = clean_db.execute(
            "SELECT addr FROM reusable_master_templates WHERE addr = %s", [addr]
        ).fetchone()
        assert row is not None
        slaves = clean_db.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction", [addr]
        ).fetchall()
        assert len(slaves) == 2
        s1 = next(s for s in slaves if s["instruction"] == "do a")
        s2 = next(s for s in slaves if s["instruction"] == "do b")
        assert s1["deps"] is None or len(s1["deps"]) == 0
        assert len(s2["deps"]) == 1
        assert s2["deps"][0] == s1["addr"]

    def test_create_with_reference(self, clean_db):
        dsl = "START -> (id='1', instruction='task') -> (id='1') -> END"
        # self-reference is illegal → SyntaxError
        with pytest.raises(SyntaxError):
            create_from_serial(dsl, conn=clean_db)

    def test_create_with_name(self, clean_db):
        dsl = "START -> (id='1', instruction='run') -> END"
        addr = create_from_serial(dsl, name="cool_template", conn=clean_db)
        name_row = clean_db.execute(
            "SELECT name FROM names WHERE addr = %s", [addr]
        ).fetchone()
        assert name_row["name"] == "cool_template"

# -------------------------------------------------------------------
# create_from_master Tests
# -------------------------------------------------------------------
class TestCreateFromMaster:
    def test_basic_master(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('top task', NULL, NULL, 'master_result')")
        master_addr = conn.execute(
            "SELECT addr FROM masters WHERE result_addr = (SELECT resolve_name('master_result'))"
        ).fetchone()["addr"]
        conn.execute("""
            SELECT new_slave(%s, 'step 1', 'slave1', NULL, NULL, 'r1', NULL, 'general')
        """, [master_addr])
        r1_addr = conn.execute("SELECT resolve_name('r1')").fetchone()["resolve_name"]
        conn.execute("""
            SELECT new_slave(%s, 'step 2', 'slave2', ARRAY[%s], NULL, 'r2', NULL, 'general')
        """, [master_addr, r1_addr])
        rmt_addr = create_from_master(master_addr, name="from_master", conn=clean_db)
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction",
            [rmt_addr]
        ).fetchall()
        assert len(slaves) == 2
        s1 = next(s for s in slaves if s["instruction"] == "step 1")
        s2 = next(s for s in slaves if s["instruction"] == "step 2")
        assert s1["deps"] is None or len(s1["deps"]) == 0
        assert len(s2["deps"]) == 1
        assert s2["deps"][0] == s1["addr"]

    def test_planner_removal(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('complex task')")
        master_addr = conn.execute(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        ).fetchone()["addr"]
        conn.execute("""
            SELECT new_slave(%s, 'plan stuff', 'planner_123', NULL, NULL, NULL, NULL, 'task')
        """, [master_addr])
        conn.execute("""
            SELECT new_slave(%s, 'execute stuff', 'worker', NULL, NULL, NULL, NULL, 'general')
        """, [master_addr])
        rmt_addr = create_from_master(master_addr, conn=clean_db)   # no name → no name insert
        slaves = conn.execute(
            "SELECT instruction FROM rmt_slaves WHERE template_addr = %s", [rmt_addr]
        ).fetchall()
        instructions = [s["instruction"] for s in slaves]
        assert "plan stuff" not in instructions
        assert "execute stuff" in instructions

# -------------------------------------------------------------------
# create_from_range Tests
# -------------------------------------------------------------------
class TestCreateFromRange:
    def test_basic_range(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('range test', NULL, NULL, 'm_res')")
        master_addr = conn.execute(
            "SELECT addr FROM masters WHERE result_addr = (SELECT resolve_name('m_res'))"
        ).fetchone()["addr"]
        conn.execute("SELECT new_slave(%s, 'A', 'sA', NULL, NULL, 'rA')", [master_addr])
        rA = conn.execute("SELECT resolve_name('rA')").fetchone()["resolve_name"]
        conn.execute("SELECT new_slave(%s, 'B', 'sB', ARRAY[%s], NULL, 'rB')", [master_addr, rA])
        rB = conn.execute("SELECT resolve_name('rB')").fetchone()["resolve_name"]
        conn.execute("SELECT new_slave(%s, 'C', 'sC', ARRAY[%s], NULL, 'rC')", [master_addr, rB])
        sA_addr = conn.execute("SELECT resolve_name('sA')").fetchone()["resolve_name"]
        sC_addr = conn.execute("SELECT resolve_name('sC')").fetchone()["resolve_name"]
        rmt_addr = create_from_range(sA_addr, sC_addr, name="range_test", conn=clean_db)
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction",
            [rmt_addr]
        ).fetchall()
        assert len(slaves) == 3
        a = next(s for s in slaves if s["instruction"] == "A")
        b = next(s for s in slaves if s["instruction"] == "B")
        c = next(s for s in slaves if s["instruction"] == "C")
        assert b["deps"] == [a["addr"]]
        assert c["deps"] == [b["addr"]]

    def test_range_with_no_path(self, clean_db):
        conn = clean_db
        conn.execute("SELECT new_master('isolated')")
        master_addr = conn.execute(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        ).fetchone()["addr"]
        conn.execute("SELECT new_slave(%s, 'X', 'sX', NULL, NULL, 'rX')", [master_addr])
        conn.execute("SELECT new_slave(%s, 'Y', 'sY', NULL, NULL, 'rY')", [master_addr])
        sX = conn.execute("SELECT resolve_name('sX')").fetchone()["resolve_name"]
        sY = conn.execute("SELECT resolve_name('sY')").fetchone()["resolve_name"]
        with pytest.raises(Exception):
            create_from_range(sX, sY, conn=clean_db)

# -------------------------------------------------------------------
# delete_node Tests
# -------------------------------------------------------------------
class TestDeleteNode:
    def test_delete_without_concatenation(self, clean_db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=clean_db)
        conn = clean_db
        node2 = conn.execute(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'",
            [addr]
        ).fetchone()["addr"]
        delete_node(node2, concatenate=False, conn=clean_db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s",
            [addr]
        ).fetchall()
        instructions = {r["instruction"] for r in remaining}
        assert instructions == {"A", "C"}
        nodeC = next(r for r in remaining if r["instruction"] == "C")
        assert nodeC["deps"] is None or len(nodeC["deps"]) == 0

    def test_delete_with_concatenation(self, clean_db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=clean_db)
        conn = clean_db
        nodeB_addr = conn.execute(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'",
            [addr]
        ).fetchone()["addr"]
        nodeA_addr = conn.execute(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'A'",
            [addr]
        ).fetchone()["addr"]
        delete_node(nodeB_addr, concatenate=True, conn=clean_db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s",
            [addr]
        ).fetchall()
        nodeC = next(r for r in remaining if r["instruction"] == "C")
        assert nodeC["deps"] == [nodeA_addr]

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
            "SELECT instruction, scope FROM slaves WHERE master_addr = %s ORDER BY instruction",
            [master["addr"]]
        ).fetchall()
        assert len(slaves) == 2
        assert slaves[0]["instruction"] == "step1"
        assert slaves[1]["instruction"] == "step2"

    def test_activation_with_placeholders(self, clean_db):
        dsl = "START -> (id='1', instruction='Add \"CODE ${{color}}\" to the master result') -> END"
        rmt_addr = create_from_serial(dsl, conn=clean_db)
        activate_as_master(rmt_addr, inputs={"color": "GREEN"}, conn=clean_db)
        conn = clean_db
        slave = conn.execute(
            "SELECT instruction FROM slaves WHERE master_addr = (SELECT addr FROM masters WHERE instruction = 'NONE' LIMIT 1)"
        ).fetchone()
        assert slave is not None
        assert "GREEN" in slave["instruction"]
        assert "${{color}}" not in slave["instruction"]

    def test_activation_with_external_deps(self, clean_db):
        conn = clean_db
        conn.execute("INSERT INTO results (addr, ready, content_str) VALUES (new_addr(), TRUE, 'pre-existing')")
        ext_result = conn.execute("SELECT currval('global_next_id')").fetchone()["currval"]
        dsl = "START -> (id='1', instruction='use external') -> END"
        rmt_addr = create_from_serial(dsl, conn=clean_db)
        activate_as_master(rmt_addr, depends_on=[ext_result], conn=clean_db)
        master = conn.execute(
            "SELECT m.addr FROM masters m JOIN master_req mr ON m.addr = mr.master_addr WHERE mr.req_addr = %s",
            [ext_result]
        ).fetchone()
        assert master is not None

# -------------------------------------------------------------------
# Missing / Not Implemented Yet
# -------------------------------------------------------------------
def test_insert_node_not_implemented():
    from python.rmt.main import insert_node
    assert callable(insert_node)

# -------------------------------------------------------------------
# Additional DSL edge cases
# -------------------------------------------------------------------
def test_parse_empty_lines():
    expr = "\nSTART -> (id='1', instruction='hi') -> END\n\n"
    result = parse(expr)
    assert len(result) == 1

def test_parse_scope():
    expr = "START -> (id='1', instruction='do', scope='task') -> END"
    result = parse(expr)
    assert result[0].scope == "task"
