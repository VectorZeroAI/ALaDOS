#!/usr/bin/env python3
"""
Comprehensive tests for built‑in tools.
Uses a transaction‑rolled‑back test database and mocks external services.
"""

import json
from datetime import datetime
from unittest.mock import patch

import pytest

from python.executor.builtins import (
    k_create, k_read, k_edit,
    context_add, context_window_land_by_addr, context_window_size_change,
    move_window_anchor, unload_item,
    add_slave, master_result_add, result_write,
    create_tool, edit_tool, execute_tool_builtin_func,
    report_paradoxal_information,
    add_cronjob,
    rmt_create_from_serial, rmt_serialise, rmt_activate_as_master,
    rmt_insert_node, rmt_delete_node, rmt_edit_instruction, rmt_change_scope,
    tool_rmt_create_from_master, create_master, rmt_create_from_range,
    rmt_edit_description,
    tool_create_result_via_event,
    tool_register_event_reaction_rmt,
    tool_register_event_reaction_execute_slave,
    web_searcher_function_fulltext, search_for_urls, web_request, web_post,
)
from python.executor.types import _ExecToolMetaData, ParadoxDetected
from python.utils.name_resolver import resolve_to_addr

from .conftest import db, meta, unique_name  # noqa: F401 – fixtures imported


class TestKnowledgeTools:
    def test_k_create_and_read(self, meta):
        name = unique_name("kn")
        res = k_create(content="hello", description="desc", name=name, _meta=meta)
        addr = int(res)
        assert addr > 0
        read_res = k_read(id=name, _meta=meta)
        assert "hello" in read_res

    def test_k_edit_content(self, meta):
        name = unique_name("kn_edit")
        k_create(content="old text", description="desc", name=name, _meta=meta)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        sr = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
        k_edit(id=name, content_change=sr, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        content = meta.conn.execute_fetchval("SELECT content FROM knowledge WHERE addr=%s", (addr,))
        assert content == "new text"

    def test_k_edit_description(self, meta):
        name = unique_name("kn_desc")
        k_create(content="c", description="old d", name=name, _meta=meta)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        sr = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
        k_edit(id=name, description_change=sr, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        desc = meta.conn.execute_fetchval("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert desc == "new d"

    def test_k_edit_both(self, meta):
        name = unique_name("kn_both")
        k_create(content="old c", description="old d", name=name, _meta=meta)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        sr = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
        k_edit(id=name, content_change=sr, description_change=sr, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        row = meta.conn.execute(
            "SELECT k.content, v.description FROM knowledge k JOIN vector_ops v ON k.addr = v.addr WHERE k.addr = %s",
            (addr,)
        ).fetchone()
        assert row[0] == "new c"
        assert row[1] == "new d"

    def test_k_read_nonexistent(self, meta):
        with pytest.raises(RuntimeError, match="resolution failed"):
            k_read(id="nonexistent", _meta=meta)


class TestContextTools:
    def test_context_add(self, meta):
        name = unique_name("ctx")
        k_create(content="c", description="d", name=name, _meta=meta)
        res = context_add(id=name, _meta=meta)
        assert res == ""
        addr = resolve_to_addr(name, meta.conn)
        cnt = meta.conn.execute_fetchval(
            "SELECT count(*) FROM master_load WHERE master_addr=%s AND item_addr=%s",
            (meta.master_id, addr)
        )
        assert cnt == 1

    def test_context_unload_item(self, meta):
        name = unique_name("unload")
        k_create(content="u", description="d", name=name, _meta=meta)
        context_add(id=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        res = unload_item(id=name, _meta=meta)
        assert res == ""
        cnt = meta.conn.execute_fetchval(
            "SELECT count(*) FROM master_load WHERE master_addr=%s AND item_addr=%s",
            (meta.master_id, addr)
        )
        assert cnt == 0

    def test_window_land_knowledge(self, meta):
        name = unique_name("anchor_k")
        k_create(content="x", description="d", name=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        meta.conn.execute(
            "UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s",
            (addr,)
        )
        context_window_land_by_addr(id=addr, _meta=meta)
        anchor = meta.conn.execute_fetchval(
            "SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta.master_id,)
        )
        assert anchor == addr

    def test_window_land_executable(self, meta):
        name = unique_name("anchor_e")
        create_tool(description="d", header="h", body="b", name=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        meta.conn.execute(
            "UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s",
            (addr,)
        )
        context_window_land_by_addr(id=addr, _meta=meta)
        anchor = meta.conn.execute_fetchval(
            "SELECT window_anchor_exe FROM master_context WHERE addr=%s", (meta.master_id,)
        )
        assert anchor == addr

    def test_window_size_change(self, meta):
        name = unique_name("sz")
        k_create(content="s", description="d", name=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        meta.conn.execute(
            "UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s",
            (addr,)
        )
        context_window_land_by_addr(id=addr, _meta=meta)
        context_window_size_change(left=5, right=-2, _meta=meta)
        row = meta.conn.execute(
            "SELECT window_size_l, window_size_r FROM master_context WHERE addr=%s", (meta.master_id,)
        ).fetchone()
        assert row[0] == 17
        assert row[1] == 10

    def test_move_anchor(self, meta):
        conn = meta.conn
        items = []
        for i in range(3):
            name = unique_name(f"mv_{i}")
            k_create(content=f"item{i}", description=f"desc{i}", name=name, _meta=meta)
            addr = resolve_to_addr(name, conn)
            conn.execute(
                "UPDATE vector_ops SET emb = array_fill(%s::float, ARRAY[768])::vector(768), position = %s WHERE addr = %s",
                (i * 0.1, 100 + i * 100, addr)
            )
            items.append(addr)
        context_window_land_by_addr(id=items[1], _meta=meta)
        move_window_anchor(amount=-1, _meta=meta)
        new_anchor = conn.execute_fetchval(
            "SELECT COALESCE(window_anchor_knowledge, window_anchor_exe) FROM master_context WHERE addr=%s",
            (meta.master_id,)
        )
        assert new_anchor == items[0]


class TestGoalTools:
    def test_add_slave_no_requires(self, meta):
        res = add_slave(instruction="do task", _meta=meta)
        addr = int(res)
        assert addr > 0
        assert meta.conn.execute_fetchval(
            "SELECT count(*) FROM slaves WHERE addr = %s", (addr,)
        ) == 1

    def test_add_slave_with_requires(self, meta):
        conn = meta.conn
        req_name = unique_name("req")
        req_addr = conn.execute_fetchval("SELECT new_addr()")
        conn.execute("INSERT INTO results (addr, ready) VALUES (%s, false)", (req_addr,))
        conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (req_addr, req_name))
        res = add_slave(instruction="dep", required_results_ids=[req_name], _meta=meta)
        slave_addr = int(res)
        req_addr_found = conn.execute_fetchval("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,))
        assert req_addr_found == req_addr

    def test_add_slave_self_requires(self, meta):
        conn = meta.conn
        result_addr = conn.execute_fetchval(
            "SELECT result_addr FROM slaves WHERE addr = %s", (meta.slave_id,)
        )
        res = add_slave(instruction="self dep", required_results_ids=['self'], _meta=meta)
        slave_addr = int(res)
        req_rel = conn.execute_fetchval("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,))
        assert req_rel == result_addr

    def test_add_slave_planner_type(self, meta):
        res = add_slave(instruction="do plan", slave_type="planner", _meta=meta) # FIXME: Move this behaviour from builtins, e.g. syscalls to DB tool side.
        assert res == ""
        # Verify a planner slave was created
        cnt = meta.conn.execute_fetchval(
            "SELECT count(*) FROM slaves WHERE instruction='do plan' AND scope='task'"
        )
        assert cnt == 1

    def test_master_result_add(self, meta):
        master_result_add(text="summary", _meta=meta)
        mc = meta.conn.execute_fetchval("SELECT master_result FROM master_context WHERE addr=%s", (meta.master_id,))
        assert "summary" in mc

    def test_result_write(self, meta):
        res = result_write(text="direct", _meta=meta)
        assert "direct" in res

    def test_create_master_tool(self, meta):
        res = create_master(instruction="new master task", _meta=meta, result_name="m_res")
        addr = int(res)
        assert addr > 0
        master_instr = meta.conn.execute_fetchval("SELECT instruction FROM masters WHERE addr=%s", (addr,))
        assert master_instr == "new master task"


class TestToolTools:
    def test_create_tool(self, meta):
        name = unique_name("tool")
        res = create_tool(description="desc", header="usage", body="print(1)", name=name, _meta=meta)
        addr = int(res)
        assert addr > 0
        header = meta.conn.execute_fetchval("SELECT header FROM executables WHERE addr=%s", (addr,))
        assert header == "usage"

    def test_create_tool_no_name(self, meta):
        res = create_tool(description="d", header="h", body="b", name=None, _meta=meta)
        addr = int(res)
        assert addr > 0
        assert meta.conn.execute_fetchval("SELECT count(*) FROM executables WHERE header='h'") == 1

    def test_edit_tool(self, meta):
        name = unique_name("tool_edit")
        create_tool(description="desc", header="old h", body="old b", name=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        sr = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
        edit_tool(id=name, header_change=sr, body_change=sr, _meta=meta)
        new_header = meta.conn.execute_fetchval("SELECT header FROM executables WHERE addr=%s", (addr,))
        new_body = meta.conn.execute_fetchval("SELECT body FROM executables WHERE addr=%s", (addr,))
        assert new_header == "new h"
        assert new_body == "new b"

    def test_edit_tool_description_only(self, meta):
        name = unique_name("tool_desc")
        create_tool(description="old desc", header="h", body="b", name=name, _meta=meta)
        addr = resolve_to_addr(name, meta.conn)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        edit_tool(id=name, new_description="new desc", _meta=meta)
        desc = meta.conn.execute_fetchval("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert desc == "new desc"

    def test_edit_tool_no_changes_raises(self, meta):
        name = unique_name("tool_nochg")
        create_tool(description="d", header="h", body="b", name=name, _meta=meta)
        with pytest.raises(TypeError, match="No change provided"):
            edit_tool(id=name, _meta=meta)

    def test_execute_tool(self, meta):
        name = unique_name("exec_tool")
        create_tool(
            description="d",
            header="h",
            body='import os, json, sys; print(json.dumps({"res": json.load(sys.stdin)}))',
            name=name,
            _meta=meta
        )
        res = execute_tool_builtin_func(id=name, kwargs={"key": "val"}, _meta=meta)
        assert "Executed tool, and got output:" in res
        # Extract the JSON part after the prefix
        output_json = res.split("Executed tool, and got output:", 1)[1].split(";")[0].strip()
        data = json.loads(output_json)
        assert data["res"] == {"key": "val"}

    def test_create_tool_timeout(self, meta):
        name = unique_name("timeout_tool")
        create_tool(description="d", header="h", body="import time; time.sleep(5)", name=name, _meta=meta)
        with pytest.raises(TimeoutError, match="Process Timed out"):
            execute_tool_builtin_func(id=name, timeout=1, _meta=meta)


class TestWebTools:
    @patch('python.executor.builtins.searcher_obj.search_website_content', return_value="mock fulltext")
    def test_web_search_fulltext(self, mock_search, meta):
        res = web_searcher_function_fulltext(query="q", _meta=meta)
        assert "mock fulltext" in res

    @patch('python.executor.builtins.searcher_obj.search', return_value=[
        {"url": "http://ex.com", "title": "T", "snippet": "S"}
    ])
    def test_web_search(self, mock_search, meta):
        res = search_for_urls(query="q", amount_results=1, _meta=meta)
        assert "http://ex.com" in res

    @patch('python.executor.builtins.httpsystem.get', return_value={
        "url": "http://ex.com", "text": "extracted", "status_code": 200, "content_raw": "raw"
    })
    def test_web_get(self, mock_get, meta):
        res = web_request(url="http://ex.com", _meta=meta)
        assert "extracted" in res

    @patch('python.executor.builtins.httpsystem.post', return_value={
        "url": "http://ex.com", "text": "post text", "status_code": 201, "content_raw": "raw"
    })
    def test_web_post_default(self, mock_post, meta):
        res = web_post(url="http://ex.com", _meta=meta)
        assert "post text" in res

    @patch('python.executor.builtins.httpsystem.post', return_value={
        "url": "http://ex.com", "status_code": 200, "content_raw": ""
    })
    def test_web_post_status_only(self, mock_post, meta):
        res = web_post(url="http://ex.com", return_type='status_code', _meta=meta)
        assert "status_code" in res


class TestCronjob:
    def test_add_cronjob_once(self, meta):
        res = add_cronjob(
            cronjob_type='once',
            action='do_this_later',
            time_between_runs=60,
            params={'ai_instruction': 'test'},
            _meta=meta
        )
        addr = int(res)
        # Verify insertion in cronjob_once
        row = meta.conn.execute(
            "SELECT name, args, start_after FROM cronjob_once WHERE addr = %s", (addr,)
        ).fetchone()
        assert row is not None
        assert row[0] == 'do_this_later'
        assert row[1] == {'ai_instruction': 'test'}

    def test_add_cronjob_loop(self, meta):
        res = add_cronjob(
            cronjob_type='loop',
            action='do_this_later',
            time_between_runs=10,
            params={'ai_instruction': 'loop'},
            _meta=meta
        )
        addr = int(res)
        row = meta.conn.execute(
            "SELECT name, args, execute_every FROM cronjob_loop WHERE addr = %s", (addr,)
        ).fetchone()
        assert row is not None
        assert row[0] == 'do_this_later'
        assert row[1] == {'ai_instruction': 'loop'}
        assert row[2] == 10


class TestRmtTools:
    def test_rmt_create_from_serial(self, meta):
        dsl = "START -> (instruction='a') -> (instruction='b') -> END"
        name = unique_name("rmt_ser")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        ser = rmt_serialise(id=rmt_addr, _meta=meta)
        assert "a" in ser and "b" in ser

    def test_rmt_serialise(self, meta):
        dsl = "START -> (instruction='x') -> END"
        name = unique_name("rmt_ser2")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        ser = rmt_serialise(id=rmt_addr, _meta=meta)
        assert "x" in ser

    def test_rmt_create_from_master(self, meta):
        conn = meta.conn
        m_addr = conn.execute_fetchval("SELECT new_master('convert me')")
        conn.execute("SELECT new_slave(%s, 'step1', 's1', NULL, NULL, 'res1')", (m_addr,))
        r1 = conn.execute_fetchval("SELECT resolve_name('res1')")
        conn.execute("SELECT new_slave(%s, 'step2', 's2', ARRAY[%s], NULL, 'res2')", (m_addr, r1))
        rmt_name = unique_name("from_master")
        res = tool_rmt_create_from_master(master_id=m_addr, name=rmt_name, _meta=meta, description="desc")
        rmt_addr = int(res)
        assert rmt_addr > 0
        slaves = conn.execute(
            "SELECT instruction FROM rmt_slaves WHERE template_addr=%s", (rmt_addr,)
        ).fetchall()
        instructions = {r[0] for r in slaves}
        assert instructions == {"step1", "step2"}

    def test_rmt_create_from_range(self, meta):
        conn = meta.conn
        m_addr = conn.execute_fetchval("SELECT new_master('range')")
        conn.execute("SELECT new_slave(%s, 'A', 'sA', NULL, NULL, 'rA')", (m_addr,))
        rA = conn.execute_fetchval("SELECT resolve_name('rA')")
        conn.execute("SELECT new_slave(%s, 'B', 'sB', ARRAY[%s], NULL, 'rB')", (m_addr, rA))
        sA = conn.execute_fetchval("SELECT resolve_name('sA')")
        sB = conn.execute_fetchval("SELECT resolve_name('sB')")
        rmt_name = unique_name("range_rmt")
        res = rmt_create_from_range(start_id=sA, end_id=sB, _meta=meta, description="desc", name=rmt_name)
        rmt_addr = int(res)
        slaves = conn.execute(
            "SELECT instruction FROM rmt_slaves WHERE template_addr=%s", (rmt_addr,)
        ).fetchall()
        instructions = {r[0] for r in slaves}
        assert instructions == {"A", "B"}

    def test_rmt_edit_description(self, meta):
        dsl = "START -> (instruction='a') -> END"
        name = unique_name("rmt_desc")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="old desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        rmt_edit_description(rmt_id=rmt_addr, new_description="new desc", _meta=meta)
        desc = meta.conn.execute_fetchval("SELECT description FROM vector_ops WHERE addr=%s", (rmt_addr,))
        assert desc == "new desc"

    def test_rmt_insert_and_delete_node(self, meta):
        dsl = "START -> (id='n1', instruction='init') -> END"
        name = unique_name("rmt_edit")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        node_addr = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='init'", (rmt_addr,)
        )
        node_name = unique_name("n1_name")
        meta.conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        new_name = unique_name("new_node")
        rmt_insert_node(rmt_id=rmt_addr, instruction="new", name=new_name, depends_on=[node_name], _meta=meta)
        new_addr = resolve_to_addr(new_name, meta.conn)
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        rmt_delete_node(rmt_slave_id=new_addr, template_id=rmt_addr, concatenate=False, _meta=meta)
        cnt = meta.conn.execute_fetchval(
            "SELECT count(*) FROM rmt_slaves WHERE template_addr=%s AND instruction='new'", (rmt_addr,)
        )
        assert cnt == 0

    def test_rmt_insert_node_with_required_by(self, meta):
        dsl = "START -> (id='n1', instruction='first') -> END"
        name = unique_name("rmt_reqby")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        node1_addr = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='first'", (rmt_addr,)
        )
        node1_name = unique_name("n1_name")
        meta.conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node1_addr, node1_name))
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        new_name = unique_name("new_reqby")
        rmt_insert_node(rmt_id=rmt_addr, instruction="second", name=new_name,
                        required_by=[node1_name], _meta=meta)
        deps = meta.conn.execute_fetchval(
            "SELECT deps FROM rmt_slaves WHERE addr=%s", (node1_addr,)
        )
        new_addr = resolve_to_addr(new_name, meta.conn)
        assert new_addr in deps

    def test_rmt_activate_as_master(self, meta):
        dsl = "START -> (instruction='do work') -> END"
        name = unique_name("rmt_act")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        rmt_activate_as_master(rmt_id=rmt_addr, inputs={}, _meta=meta)
        slave_count = meta.conn.execute_fetchval(
            "SELECT count(*) FROM slaves WHERE instruction='do work' AND master_addr IN "
            "(SELECT addr FROM masters WHERE instruction='NONE')"
        )
        assert slave_count == 1

    def test_rmt_activate_with_inputs(self, meta):
        dsl = "START -> (instruction='Use ${{color}}') -> END"
        name = unique_name("rmt_inputs")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        rmt_activate_as_master(rmt_id=rmt_addr, inputs={"color": "blue"}, _meta=meta)
        instr = meta.conn.execute_fetchval(
            "SELECT instruction FROM slaves WHERE instruction LIKE '%%blue%%'"
        )
        assert instr == "Use blue"

    def test_rmt_edit_instruction(self, meta):
        dsl = "START -> (id='editme', instruction='old text') -> END"
        name = unique_name("rmt_instr")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        node_addr = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='old text'", (rmt_addr,)
        )
        node_name = unique_name("editme_name")
        meta.conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        rmt_edit_instruction(node_id=node_name, sr_block="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>", _meta=meta)
        new_instr = meta.conn.execute_fetchval("SELECT instruction FROM rmt_slaves WHERE addr=%s", (node_addr,))
        assert new_instr == "new text"

    def test_rmt_change_scope(self, meta):
        dsl = "START -> (id='sc', instruction='t', scope='general') -> END"
        name = unique_name("rmt_scope")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        node_addr = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='t'", (rmt_addr,)
        )
        node_name = unique_name("sc_name")
        meta.conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        rmt_change_scope(node_id=node_name, new_scope='task', _meta=meta)
        scope = meta.conn.execute_fetchval("SELECT scope FROM rmt_slaves WHERE addr=%s", (node_addr,))
        assert scope == 'task'

    def test_rmt_delete_node_concatenate(self, meta):
        dsl = "START -> (id='1', instruction='A') -> (id='2', instruction='B') -> (id='3', instruction='C') -> END"
        name = unique_name("rmt_cat")
        rmt_create_from_serial(dsl=dsl, name=name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(name, meta.conn)
        nodeB = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='B'", (rmt_addr,)
        )
        nodeA = meta.conn.execute_fetchval(
            "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='A'", (rmt_addr,)
        )
        meta.occ_last_change = meta.conn.execute_fetchval("SELECT NOW()")
        rmt_delete_node(rmt_slave_id=nodeB, template_id=rmt_addr, concatenate=True, _meta=meta)
        remaining = meta.conn.execute(
            "SELECT instruction, deps FROM rmt_slaves WHERE template_addr=%s", (rmt_addr,)
        ).fetchall()
        c = next(r for r in remaining if r[0] == "C")
        assert c[1] == [nodeA]


class TestEventTools:
    def test_create_result_via_event(self, meta):
        conn = meta.conn
        str_ret = tool_create_result_via_event(
            event_path="test.event",
            result_str="data: ${{data}}",
            name="ev_res",
            _meta=meta
        )
        resolved = resolve_to_addr("ev_res", conn)
        assert str(resolved) in str_ret

    def test_register_reaction_rmt(self, meta):
        dsl = "START -> (instruction='react') -> END"
        rmt_name = unique_name("react_rmt")
        rmt_create_from_serial(dsl=dsl, name=rmt_name, _meta=meta, description="desc")
        rmt_addr = resolve_to_addr(rmt_name, meta.conn)
        consumer_addr = tool_register_event_reaction_rmt(
            event_path="ev.react",
            rmt_id=rmt_addr,
            args={"key": "val"},
            _meta=meta
        )
        conn = meta.conn
        db_consumer = conn.execute_fetchval(
            "SELECT addr FROM event_consumers WHERE event_path='ev.react'"
        )
        assert db_consumer is not None
        stored_rmt = conn.execute_fetchval(
            "SELECT rmt_addr FROM event_call_rmt WHERE addr = %s", (db_consumer,)
        )
        assert stored_rmt == rmt_addr

    def test_register_reaction_execute_slave(self, meta):
        msg = tool_register_event_reaction_execute_slave(
            event_path="ev.slave",
            instruction="do stuff",
            scope="task",
            _meta=meta
        )
        conn = meta.conn
        db_consumer = conn.execute_fetchval(
            "SELECT addr FROM event_consumers WHERE event_path='ev.slave'"
        )
        assert db_consumer is not None
        stored_instr = conn.execute_fetchval(
            "SELECT instruction FROM event_call_execute_slave WHERE addr = %s", (db_consumer,)
        )
        assert stored_instr == "do stuff"


def test_report_paradox(meta):
    with pytest.raises(ParadoxDetected):
        report_paradoxal_information(items=[1], paradox="conflict", _meta=meta)
