#!/usr/bin/env python3
"""
Test suite for the RMT (Reusable Master Template) subsystem.
"""
import pytest
from python.rmt.dsl import parse
from python.rmt.main import (
    create_from_serial,
    create_from_master,
    create_from_range,
    delete_node,
    activate_as_master,
    serialize,
)
from .conftest import db  # noqa: F401 – common fixture


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
        pass  # DSL can't express cycles, placeholder


class TestSerializationRoundTrip:
    def test_roundtrip_linear(self, db):
        dsl = "START -> (id='1', instruction='task1') -> (id='2', instruction='task2') -> END"
        addr = create_from_serial(dsl, name="test_rmt", conn=db)
        serialized = serialize(addr, conn=db)
        reparsed = parse(serialized)
        assert len(reparsed) == 2
        instructions = {n.instruction for n in reparsed}
        assert instructions == {"task1", "task2"}
        n1 = next(n for n in reparsed if n.instruction == "task1")
        n2 = next(n for n in reparsed if n.instruction == "task2")
        assert n1.deps == []
        assert n2.deps == [n1.id]


class TestCreateFromSerial:
    def test_create_linear(self, db):
        dsl = "START -> (id='1', instruction='do a') -> (id='2', instruction='do b') -> END"
        addr = create_from_serial(dsl, conn=db)
        row = db.execute("SELECT addr FROM reusable_master_templates WHERE addr = %s", [addr]).fetchone()
        assert row is not None
        slaves = db.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction", [addr]
        ).fetchall()
        assert len(slaves) == 2
        s1 = next(s for s in slaves if s[0] == "do a")
        s2 = next(s for s in slaves if s[0] == "do b")
        assert s1[1] is None or len(s1[1]) == 0
        assert len(s2[1]) == 1
        addrs = db.execute("SELECT instruction, addr FROM rmt_slaves WHERE template_addr = %s", [addr]).fetchall()
        a_addr = next(r[1] for r in addrs if r[0] == "do a")
        assert s2[1][0] == a_addr

    def test_create_with_reference(self, db):
        dsl = "START -> (id='1', instruction='task') -> (id='1') -> END"
        with pytest.raises(SyntaxError):
            create_from_serial(dsl, conn=db)

    def test_create_with_name(self, db):
        dsl = "START -> (id='1', instruction='run') -> END"
        addr = create_from_serial(dsl, name="cool_template", conn=db)
        row = db.execute("SELECT name FROM names WHERE addr = %s", [addr]).fetchone()
        assert row[0] == "cool_template"


class TestCreateFromMaster:
    def test_basic_master(self, db):
        conn = db
        conn.execute("SELECT new_master('top task', NULL, NULL, 'master_result')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT resolve_name('master_result'))"
        )
        conn.execute("SELECT new_slave(%s, 'step 1', 'slave1', NULL, NULL, 'r1', NULL, 'general')", [master_addr])
        r1_addr = conn.execute_fetchval("SELECT resolve_name('r1')")
        conn.execute("SELECT new_slave(%s, 'step 2', 'slave2', ARRAY[%s], NULL, 'r2', NULL, 'general')", [master_addr, r1_addr])
        rmt_addr = create_from_master(master_addr, name="from_master", conn=db)
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction", [rmt_addr]
        ).fetchall()
        assert len(slaves) == 2
        s1 = next(s for s in slaves if s[0] == "step 1")
        s2 = next(s for s in slaves if s[0] == "step 2")
        assert s1[1] is None or len(s1[1]) == 0
        assert len(s2[1]) == 1
        a_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'step 1'", [rmt_addr]
        )
        assert s2[1][0] == a_addr

    def test_planner_removal(self, db):
        conn = db
        conn.execute("SELECT new_master('complex task')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        )
        conn.execute("SELECT new_slave(%s, 'plan stuff', 'planner_123', NULL, NULL, NULL, NULL, 'task')", [master_addr])
        conn.execute("SELECT new_slave(%s, 'execute stuff', 'worker', NULL, NULL, NULL, NULL, 'general')", [master_addr])
        rmt_addr = create_from_master(master_addr, conn=db)
        slaves = conn.execute("SELECT instruction FROM rmt_slaves WHERE template_addr = %s", [rmt_addr]).fetchall()
        instructions = [s[0] for s in slaves]
        assert "plan stuff" not in instructions
        assert "execute stuff" in instructions


class TestCreateFromRange:
    def test_basic_range(self, db):
        conn = db
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
        rmt_addr = create_from_range(start_node_id=sA_addr, conn=db, end_node_id=sC_addr, name="range_test")
        slaves = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s ORDER BY instruction", [rmt_addr]
        ).fetchall()
        assert len(slaves) == 3
        a = next(s for s in slaves if s[0] == "A")
        b = next(s for s in slaves if s[0] == "B")
        c = next(s for s in slaves if s[0] == "C")
        a_addr = conn.execute_fetchval("SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'A'", [rmt_addr])
        b_addr = conn.execute_fetchval("SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [rmt_addr])
        assert b[1] == [a_addr]
        assert c[1] == [b_addr]

    def test_range_with_no_path(self, db):
        conn = db
        conn.execute("SELECT new_master('isolated')")
        master_addr = conn.execute_fetchval(
            "SELECT addr FROM masters WHERE result_addr = (SELECT addr FROM results WHERE metadata->>'type' = 'master' ORDER BY addr DESC LIMIT 1)"
        )
        conn.execute("SELECT new_slave(%s, 'X', 'sX', NULL, NULL, 'rX')", [master_addr])
        conn.execute("SELECT new_slave(%s, 'Y', 'sY', NULL, NULL, 'rY')", [master_addr])
        sX = conn.execute_fetchval("SELECT resolve_name('sX')")
        sY = conn.execute_fetchval("SELECT resolve_name('sY')")
        with pytest.raises(ValueError, match="Items do not intersect!"):
            create_from_range(sX, db, sY)


class TestDeleteNode:
    def test_delete_without_concatenation(self, db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=db)
        conn = db
        node2 = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [addr]
        )
        delete_node(node2, concatenate=False, conn=db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s", [addr]
        ).fetchall()
        instructions = {r[0] for r in remaining}
        assert instructions == {"A", "C"}
        nodeC = next(r for r in remaining if r[0] == "C")
        assert nodeC[1] is None or len(nodeC[1]) == 0

    def test_delete_with_concatenation(self, db):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        addr = create_from_serial(dsl, conn=db)
        conn = db
        nodeB_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'B'", [addr]
        )
        nodeA_addr = conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr = %s AND instruction = 'A'", [addr]
        )
        delete_node(nodeB_addr, concatenate=True, conn=db)
        remaining = conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr = %s", [addr]
        ).fetchall()
        nodeC = next(r for r in remaining if r[0] == "C")
        assert nodeC[1] == [nodeA_addr]


class TestActivateAsMaster:
    def test_activation_basic(self, db):
        dsl = "START -> (id='1', instruction='step1') -> (id='2', instruction='step2') -> END"
        rmt_addr = create_from_serial(dsl, name="basic_template", conn=db)
        activate_as_master(rmt_addr, inputs={}, conn=db)
        conn = db
        master = conn.execute("SELECT * FROM masters WHERE instruction = 'NONE'").fetchone()
        assert master is not None
        slaves = conn.execute(
            "SELECT instruction FROM slaves WHERE master_addr = %s ORDER BY instruction", [master[0]]
        ).fetchall()
        assert len(slaves) == 2
        assert slaves[0][0] == "step1"
        assert slaves[1][0] == "step2"

    def test_activation_with_placeholders(self, db):
        dsl = "START -> (id='1', instruction='Add \"CODE ${{color}}\" to the master result') -> END"
        rmt_addr = create_from_serial(dsl, conn=db)
        activate_as_master(rmt_addr, inputs={"color": "GREEN"}, conn=db)
        conn = db
        slave = conn.execute(
            "SELECT instruction FROM slaves WHERE master_addr = (SELECT addr FROM masters WHERE instruction = 'NONE' LIMIT 1)"
        ).fetchone()
        assert slave is not None
        assert "GREEN" in slave[0]
        assert "${{color}}" not in slave[0]

    def test_activation_with_external_deps(self, db):
        conn = db
        conn.execute("INSERT INTO results (addr, ready, content_str) VALUES (new_addr(), TRUE, 'pre-existing')")
        ext_result = conn.execute_fetchval("SELECT currval('global_next_id')")
        dsl = "START -> (id='1', instruction='use external') -> END"
        rmt_addr = create_from_serial(dsl, conn=db)
        activate_as_master(rmt_addr, depends_on=[ext_result], conn=db)
        master = conn.execute(
            "SELECT m.addr FROM masters m JOIN master_req mr ON m.addr = mr.master_addr WHERE mr.req_addr = %s",
            [ext_result]
        ).fetchone()
        assert master is not None


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
