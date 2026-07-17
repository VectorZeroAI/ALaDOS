from unittest.mock import patch
import json

import pytest

from ALaDOS.src.python.executor.builtins import (
    add_cronjob,
    add_slave,
    claim_item,
    context_add,
    context_window_land_by_addr,
    context_window_size_change,
    create_master,
    create_tool,
    edit_tool,
    execute_tool_builtin_func,
    k_create,
    k_edit,
    k_read,
    master_result_add,
    move_window_anchor,
    release_item,
    report_paradoxal_information,
    result_write,
    rmt_activate_as_master,
    rmt_change_scope,
    rmt_create_from_serial,
    rmt_delete_node,
    rmt_edit_instruction,
    rmt_insert_node,
    rmt_serialise,
    search_for_urls,
    send_message_to_human_v_webui,
    tool_create_from_master,
    web_post,
    web_request,
    web_searcher_function_fulltext,
)
from ALaDOS.src.python.executor.types import _ExecToolMetaData
from ALaDOS.src.python.utils.conn_factory import Conn, conn_factory
from ALaDOS.src.python.utils.name_resolver import resolve_to_addr

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
_unique_counter = 0

def unique_name(prefix: str = "test") -> str:
    global _unique_counter
    _unique_counter += 1
    return f"{prefix}_{_unique_counter}"

def create_test_meta(conn: Conn) -> _ExecToolMetaData:
    """Create a master+slave pair and return metadata for a tool call."""
    master_addr = conn.execute_fetchval("SELECT new_master('test_master')")
    # Add a dummy slave to satisfy some tools that need a slave context
    slave_addr = conn.execute_fetchval(
        "SELECT new_slave(%s, 'test_instruction', 'general')",
        (master_addr,)
    )
    return _ExecToolMetaData(
        master_id=master_addr,
        conn=conn,
        slave_id=slave_addr,
        context_limit=10000,
    )

# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture
def conn():
    """Return a fresh database connection with a transaction that rolls back after test."""
    db = conn_factory()
    db.autocommit = False
    db.execute("BEGIN")
    yield db
    db.execute("ROLLBACK")
    db.close()

@pytest.fixture
def meta(conn):
    """Test metadata object, tied to the rollback transaction."""
    return create_test_meta(conn)

# ----------------------------------------------------------------------
# Knowledge item tests
# ----------------------------------------------------------------------
def test_k_create(meta):
    name = unique_name("knowledge")
    res = k_create(content="sample content", description="sample desc", name=name, _meta=meta)
    assert "knowledge entry" in res
    addr = meta.conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (name,))
    content = meta.conn.execute_fetchval("SELECT content FROM knowledge WHERE addr=%s", (addr,))
    assert content == "sample content"

def test_k_read(meta):
    name = unique_name("knowledge_read")
    k_create(content="read me", description="read desc", name=name, _meta=meta)
    res = k_read(id=name, _meta=meta)
    assert "read me" in res

def test_k_edit(meta):
    name = unique_name("knowledge_edit")
    k_create(content="old content", description="old desc", name=name, _meta=meta)
    addr = resolve_to_addr(name, meta.conn)
    claim_item(item_id=addr, _meta=meta)
    sr_block = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
    k_edit(id=name, content_change=sr_block, _meta=meta)
    content = meta.conn.execute_fetchval("SELECT content FROM knowledge WHERE addr=%s", (addr,))
    assert content == "new content"

# ----------------------------------------------------------------------
# Context & window tests
# ----------------------------------------------------------------------
def test_context_add(meta):
    name = unique_name("ctx_add")
    k_create(content="ctx_item", description="ctx desc", name=name, _meta=meta)
    res = context_add(id=name, _meta=meta)
    assert "Added context" in res
    addr = resolve_to_addr(name, meta.conn)
    load = meta.conn.execute_fetchval(
        "SELECT count(*) FROM master_load WHERE master_addr=%s AND item_addr=%s",
        (meta.master_id, addr)
    )
    assert load == 1

def test_context_window_land_by_addr(meta):
    name = unique_name("anchor")
    k_create(content="anchor", description="anchor desc", name=name, _meta=meta)
    addr = resolve_to_addr(name, meta.conn)
    meta.conn.execute("UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s", (addr,))
    context_window_land_by_addr(id=addr, _meta=meta)
    anchor = meta.conn.execute_fetchval(
        "SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta.master_id,)
    )
    assert anchor == addr

def test_context_window_size_change(meta):
    name = unique_name("size_change")
    k_create(content="size", description="size desc", name=name, _meta=meta)
    addr = resolve_to_addr(name, meta.conn)
    meta.conn.execute("UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s", (addr,))
    context_window_land_by_addr(id=addr, _meta=meta)
    context_window_size_change(left=5, right=-2, _meta=meta)
    row = meta.conn.execute(
        "SELECT window_size_l, window_size_r FROM master_context WHERE addr=%s", (meta.master_id,)
    ).fetchone()
    assert row[0] == 17   # default 12 + 5
    assert row[1] == 10   # default 12 - 2

def test_move_window_anchor(meta):
    # Insert several items with embeddings so positions are calculated naturally
    conn = meta.conn
    items = []
    for i in range(3):
        name = unique_name(f"move_{i}")
        k_create(content=f"item{i}", description=f"desc{i}", name=name, _meta=meta)
        addr = resolve_to_addr(name, conn)
        conn.execute("UPDATE vector_ops SET emb = array_fill(%s::float, ARRAY[768])::vector(768) WHERE addr = %s",
                     (i * 0.1, addr))
        items.append(addr)
    # Set window anchor to the middle item
    context_window_land_by_addr(id=items[1], _meta=meta)
    # Move anchor left by 1
    move_window_anchor(amount=-1, _meta=meta)
    new_anchor = conn.execute_fetchval(
        "SELECT COALESCE(window_anchor_knowledge, window_anchor_exe) FROM master_context WHERE addr=%s",
        (meta.master_id,)
    )
    # After the fix, the positions will be distinct; the anchor should move to the previous item
    assert new_anchor == items[0]

# ----------------------------------------------------------------------
# Slave / goal tests
# ----------------------------------------------------------------------
def test_add_slave_no_requires(meta):
    res = add_slave(instruction="simple step", _meta=meta)
    assert "Added a new slave" in res
    slave = meta.conn.execute_fetchval("SELECT addr FROM slaves WHERE instruction='simple step'")
    assert slave is not None

def test_add_slave_with_requires(meta):
    conn = meta.conn
    req_name = unique_name("req")
    req_addr = conn.execute_fetchval("SELECT new_addr()")
    conn.execute("INSERT INTO results (addr, ready) VALUES (%s, false)", (req_addr,))
    conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (req_addr, req_name))
    res = add_slave(instruction="depends", required_results_ids=[req_name], _meta=meta)
    assert "Added a new slave" in res
    slave_addr = conn.execute_fetchval("SELECT addr FROM slaves WHERE instruction='depends'")
    req_rel = conn.execute_fetchval("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,))
    assert req_rel == req_addr

def test_add_slave_self_requires(meta):
    res = add_slave(instruction="self depends", required_results_ids=['self'], _meta=meta)
    assert "Added a new slave" in res
    slave_addr = meta.conn.execute_fetchval("SELECT addr FROM slaves WHERE instruction='self depends'")
    req_rel = meta.conn.execute_fetchval("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,))
    assert req_rel == meta.slave_id

def test_master_result_add(meta):
    master_result_add(text="result chunk", _meta=meta)
    mc = meta.conn.execute_fetchval("SELECT master_result FROM master_context WHERE addr=%s", (meta.master_id,))
    assert "result chunk" in mc

def test_result_write(meta):
    res = result_write(text="direct result", _meta=meta)
    assert "direct result" in res

# ----------------------------------------------------------------------
# Tools (create / edit / execute)
# ----------------------------------------------------------------------
def test_tool_create(meta):
    name = unique_name("new_tool")
    res = create_tool(
        description="test tool", header="usage", body="print('hello')",
        name=name, _meta=meta
    )
    assert "Created tool" in res
    addr = resolve_to_addr(name, meta.conn)
    header = meta.conn.execute_fetchval("SELECT header FROM executables WHERE addr=%s", (addr,))
    assert header == "usage"

def test_tool_edit(meta):
    name = unique_name("tool_edit")
    create_tool(description="edit tool", header="old header", body="old body", name=name, _meta=meta)
    addr = resolve_to_addr(name, meta.conn)
    claim_item(item_id=addr, _meta=meta)
    sr_block = "<SEARCH>old</SEARCH><REPLACE>new</REPLACE>"
    edit_tool(id=name, header_change=sr_block, body_change=sr_block, _meta=meta)
    header = meta.conn.execute_fetchval("SELECT header FROM executables WHERE addr=%s", (addr,))
    body = meta.conn.execute_fetchval("SELECT body FROM executables WHERE addr=%s", (addr,))
    assert header == "new header"
    assert body == "new body"

def test_tool_execute(meta):
    name = unique_name("exec_tool")
    create_tool(
        description="exec test",
        header="execute me",
        body="import os, json; print(json.dumps({'res': os.environ.get('KWARGS')}))",
        name=name, _meta=meta
    )
    res = execute_tool_builtin_func(id=name, kwargs={"key": "value"}, _meta=meta)
    assert "ran tools stdout" in res
    # Parse the output to extract the JSON result
    output_json = res.split("ran tools stdout: ", 1)[1]
    data = json.loads(output_json)
    assert data["res"] == json.dumps({"key": "value"})

# ----------------------------------------------------------------------
# Claim / release
# ----------------------------------------------------------------------
def test_claim_and_release(meta):
    name = unique_name("owned")
    k_create(content="owned content", description="desc", name=name, _meta=meta)
    addr = resolve_to_addr(name, meta.conn)
    claim_item(item_id=addr, _meta=meta)
    owner = meta.conn.execute_fetchval("SELECT owner FROM ownership WHERE addr=%s", (addr,))
    assert owner == meta.master_id
    release_item(item_id=addr, _meta=meta)
    count = meta.conn.execute_fetchval("SELECT count(*) FROM ownership WHERE addr=%s", (addr,))
    assert count == 0

# ----------------------------------------------------------------------
# Web search / communication
# ----------------------------------------------------------------------
@patch('ALaDOS.src.python.executor.builtins.searcher_obj.search_website_content', return_value="mock fulltext")
def test_web_search_fulltext(mock_search, meta):
    res = web_searcher_function_fulltext(query="test query", _meta=meta)
    assert "mock fulltext" in res

@patch('ALaDOS.src.python.executor.builtins.searcher_obj.search', return_value=[
    {"url": "http://example.com", "title": "Example", "snippet": "snippet"}
])
def test_web_search(mock_search, meta):
    res = search_for_urls(query="test", amount_results=1, _meta=meta)
    assert "http://example.com" in res

@patch('ALaDOS.src.python.executor.builtins.httpsystem.get', return_value={
    "url": "http://example.com", "text": "extracted text", "status_code": 200, "content_raw": "raw"
})
def test_web_get(mock_get, meta):
    res = web_request(url="http://example.com", _meta=meta)
    assert "extracted text" in res

@patch('ALaDOS.src.python.executor.builtins.httpsystem.post', return_value={
    "url": "http://example.com", "text": "post text", "status_code": 201, "content_raw": "raw"
})
def test_web_post(mock_post, meta):
    res = web_post(url="http://example.com", _meta=meta)
    assert "post text" in res

def test_user_send_message(meta):
    # Set up a minimal session context so the tool can find an ai_message result
    conn = meta.conn
    session_name = "test_session"
    # Give the master a name for the session
    conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (meta.master_id, session_name))
    # Insert an AI message result that the tool will target
    ai_msg_addr = conn.execute_fetchval("SELECT new_addr()")
    conn.execute(
        "INSERT INTO results (addr, ready, metadata) VALUES (%s, false, %s::jsonb)",
        (ai_msg_addr, json.dumps({"type": "ai_message", "session_name": session_name, "turn": 1}))
    )
    # Now the tool should succeed
    res = send_message_to_human_v_webui(text="hello", _meta=meta)
    assert "Sent a message" in res

# ----------------------------------------------------------------------
# Cronjob
# ----------------------------------------------------------------------
@patch('ALaDOS.src.python.executor.builtins.parse')
def test_add_cronjob(mock_parse, meta):
    res = add_cronjob(
        cronjob_type='once',
        cronjob_action='do_this_later',
        time_between_runs=60,
        params={'ai_instruction': 'test'},
        _meta=meta
    )
    assert "Added a cronjob" in res
    mock_parse.assert_called_once_with({
        'action': 'do_this_later',
        'cronjob_type': 'once',
        'params': {'ai_instruction': 'test'},
        'run_after_or_every_s': 60
    }, meta.conn)

# ----------------------------------------------------------------------
# RMT (Reusable Master Template) tests
# ----------------------------------------------------------------------
def test_rmt_serialise(meta):
    conn = meta.conn
    dsl = "START -> (instruction='step1') -> (instruction='step2') -> END"
    name = unique_name("rmt_serial")
    rmt_create_from_serial(dsl=dsl, name=name, _meta=meta)
    rmt_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (name,))
    serial = rmt_serialise(id=rmt_addr, _meta=meta)
    assert "step1" in serial
    assert "step2" in serial

def test_rmt_create_from_master(meta):
    conn = meta.conn
    master_name = unique_name("src_master")
    # Create a master with a planner slave + add a dummy non-planner slave
    create_master(instruction="test master", result_name=master_name, _meta=meta)
    master_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (master_name,))
    # Add a dummy slave that won't be filtered out
    conn.execute("SELECT new_slave(%s, 'keep_me', 'general')", (master_addr,))
    rmt_name = unique_name("rmt_from_master")
    res = tool_create_from_master(master_id=master_addr, name=rmt_name, _meta=meta)
    assert "Created rmt from master" in res
    rmt_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (rmt_name,))
    count = conn.execute_fetchval(
        "SELECT count(*) FROM rmt_slaves WHERE template_addr=%s", (rmt_addr,)
    )
    # Should contain at least the dummy slave
    assert count > 0

def test_rmt_insert_and_delete_node(meta):
    conn = meta.conn
    dsl = "START -> (id='n1', instruction='initial') -> END"
    rmt_name = unique_name("rmt_edit")
    rmt_create_from_serial(dsl=dsl, name=rmt_name, _meta=meta)
    rmt_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (rmt_name,))
    # Get addr of the existing node by instruction
    node_addr = conn.execute_fetchval(
        "SELECT addr FROM rmt_slaves WHERE template_addr=%s AND instruction='initial'", (rmt_addr,)
    )
    # Create a real name for that node so we can refer to it in depends_on
    node_name = unique_name("node_n1")
    conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
    res = rmt_insert_node(
        rmt_id=rmt_addr,
        instruction="new node",
        name=unique_name("new_node"),
        depends_on=[node_name],
        _meta=meta
    )
    assert "Inserted rmt node" in res
    # Delete the newly inserted node (by its name)
    new_node_name = unique_name("new_node")
    new_node_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (new_node_name,))
    rmt_delete_node(node_id=new_node_addr, concatenate=False, _meta=meta)
    count = conn.execute_fetchval(
        "SELECT count(*) FROM rmt_slaves WHERE template_addr=%s AND instruction='new node'",
        (rmt_addr,)
    )
    assert count == 0

def test_rmt_activate_as_master(meta):
    conn = meta.conn
    dsl = "START -> (instruction='do work') -> END"
    rmt_name = unique_name("rmt_activate")
    rmt_create_from_serial(dsl=dsl, name=rmt_name, _meta=meta)
    rmt_addr = conn.execute_fetchval("SELECT addr FROM names WHERE name=%s", (rmt_name,))
    rmt_activate_as_master(rmt_id=rmt_addr, inputs={}, _meta=meta)
    # The master created by activate_as_master has instruction 'NONE'.
    # The slave with instruction 'do work' should exist.
    slave_count = conn.execute_fetchval(
        "SELECT count(*) FROM slaves WHERE instruction='do work' AND master_addr IN "
        "(SELECT addr FROM masters WHERE instruction='NONE')"
    )
    assert slave_count == 1

def test_rmt_edit_instruction(meta):
    conn = meta.conn
    dsl = "START -> (id='editme', instruction='old text') -> END"
    rmt_name = unique_name("rmt_edit_instr")
    rmt_create_from_serial(dsl=dsl, name=rmt_name, _meta=meta)
    node_addr = conn.execute_fetchval(
        "SELECT addr FROM rmt_slaves WHERE template_addr=(SELECT addr FROM names WHERE name=%s) AND instruction='old text'",
        (rmt_name,)
    )
    node_name = unique_name("editme_name")
    conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
    rmt_edit_instruction(node_id=node_name, sr_block="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>", _meta=meta)
    new_instr = conn.execute_fetchval("SELECT instruction FROM rmt_slaves WHERE addr=%s", (node_addr,))
    assert new_instr == "new text"

def test_rmt_change_scope(meta):
    conn = meta.conn
    dsl = "START -> (id='scope_node', instruction='test', scope='general') -> END"
    rmt_name = unique_name("rmt_scope")
    rmt_create_from_serial(dsl=dsl, name=rmt_name, _meta=meta)
    node_addr = conn.execute_fetchval(
        "SELECT addr FROM rmt_slaves WHERE template_addr=(SELECT addr FROM names WHERE name=%s) AND instruction='test'",
        (rmt_name,)
    )
    node_name = unique_name("scope_node_name")
    conn.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (node_addr, node_name))
    rmt_change_scope(node_id=node_name, new_scope='task', _meta=meta)
    scope = conn.execute_fetchval("SELECT scope FROM rmt_slaves WHERE addr=%s", (node_addr,))
    assert scope == 'task'

# ----------------------------------------------------------------------
# Paradox & Error tests
# ----------------------------------------------------------------------
def test_report_paradoxal_information(meta):
    with pytest.raises(Exception):   # ParadoxDetected
        report_paradoxal_information(
            items=["addr1"], paradox="conflicting info", _meta=meta
        )
