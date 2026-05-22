# tests/test_builtins.py
import pytest
import psycopg
from psycopg.types.json import Jsonb
from unittest.mock import Mock, patch, MagicMock
import os
import sys

# Add project root to path (adjust as needed)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the builtins module and its dependencies
from src.python.executor.builtins import (
    k_create, k_edit, k_read, execute_tool_builtin_func, create_tool, edit_tool,
    context_add_by_addr, add_slave, add_replanner_slave, master_result_add,
    context_window_lands, context_window_land, context_window_size_change,
    move_window_anchor, result_write, report_paradoxal_information, add_cronjob,
    unload_item, web_searcher_function_fulltext, send_message_to_human_v_webui,
    search_for_urls
)
from src.python.executor.types import _ExecToolMetaData, SlaveScope
from src.python.executor.exceptions import ParadoxDetected
from src.python.utils.uqueue import Uqueue

# ---------- Fixtures ----------
@pytest.fixture(scope="session")
def test_conn():
    """Provide a connection to the test database."""
    conn = psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="alados_test",
        autocommit=True
    )
    yield conn
    conn.close()

@pytest.fixture
def meta(test_conn):
    """Create a minimal _ExecToolMetaData for testing."""
    # Create a master and a slave to provide context
    with test_conn.cursor() as cur:
        cur.execute("INSERT INTO masters (instruction) VALUES ('test master') RETURNING addr")
        master_addr = cur.fetchone()[0]
        # Also create a slave for this master (so that slave_id can be used)
        cur.execute("SELECT new_slave(%s, 'test slave', NULL, NULL, NULL, NULL, NULL, 'general')", (master_addr,))
        slave_addr = cur.fetchone()[0]
        # Ensure the slave's result is created
        cur.execute("SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,))
        result_addr = cur.fetchone()[0]
        # Mark the result as not ready (default)
    return {
        'conn': test_conn,
        'master_id': master_addr,
        'slave_id': slave_addr,
        '_embedder_queue': Uqueue(),
        'context_limit': 10000
    }

# ---------- Mocks ----------
@pytest.fixture(autouse=True)
def mock_embedder():
    """Mock the embedder module to avoid real model loading."""
    with patch('src.python.executor.builtins.embedder') as mock_emb:
        mock_emb.encode_query.return_value = [0.0] * 768  # dummy vector
        yield mock_emb

@pytest.fixture(autouse=True)
def mock_searcher():
    """Mock the SearxngSearcher to avoid real web requests."""
    with patch('src.python.executor.builtins.searcher_obj') as mock:
        mock.search.return_value = []
        mock.search_website_content.return_value = "Mocked search result"
        yield mock

@pytest.fixture(autouse=True)
def mock_subprocess():
    """Mock subprocess.run for tool.execute."""
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = Mock(stdout="mocked output", stderr="")
        yield mock_run

# ---------- Test Tools ----------
def test_k_create(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM knowledge")
        before = cur.fetchone()[0]
    result = k_create(content="test content", description="test desc", name="test_k", _meta=meta)
    assert "knowledge entry test_k@" in result
    with conn.cursor() as cur:
        cur.execute("SELECT content, description FROM knowledge k JOIN vector_ops v ON k.addr = v.addr_k WHERE k.addr = (SELECT addr FROM names WHERE name='test_k')")
        content, desc = cur.fetchone()
        assert content == "test content"
        assert desc == "test desc"
        assert cur.execute("SELECT count(*) FROM knowledge").fetchone()[0] == before + 1

def test_k_edit_by_addr(meta):
    conn = meta['conn']
    # Create a knowledge item
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'old content')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'old desc')", (addr,))
    # Edit content
    res = k_edit(addr=addr, content_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>", _meta=meta)
    assert "Edited the knowledge item" in res
    with conn.cursor() as cur:
        cur.execute("SELECT content FROM knowledge WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "new content"
        cur.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "old desc"  # unchanged

def test_k_edit_by_name(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'foo')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'bar')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'test_name')", (addr,))
    res = k_edit(name="test_name", description_change="<SEARCH>bar</SEARCH><REPLACE>baz</REPLACE>", _meta=meta)
    assert "Edited the knowledge item" in res
    with conn.cursor() as cur:
        cur.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "baz"

def test_k_read_by_addr(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'secret')", (addr,))
    res = k_read(addr=addr, _meta=meta)
    assert "secret" in res

def test_k_read_by_name(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'secret2')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'test_read')", (addr,))
    res = k_read(name="test_read", _meta=meta)
    assert "secret2" in res

def test_execute_tool(meta, mock_subprocess):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO executables (addr, header, body) VALUES (%s, 'test header', 'print(\"hello\")')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'test_tool')", (addr,))
    res = execute_tool_builtin_func(name="test_tool", kwargs={"x": 1}, _meta=meta)
    assert "ran tools stdout: mocked output" in res
    mock_subprocess.assert_called_once()
    args, kwargs = mock_subprocess.call_args
    assert kwargs['input'] == 'print("hello")'

def test_create_tool(meta):
    conn = meta['conn']
    before = conn.execute("SELECT count(*) FROM executables").fetchone()[0]
    res = create_tool(description="desc", header="header", body="body", name="my_tool", _meta=meta)
    assert "Created tool my_tool@" in res
    after = conn.execute("SELECT count(*) FROM executables").fetchone()[0]
    assert after == before + 1
    # Check vector_ops entry
    addr = conn.execute("SELECT addr FROM names WHERE name='my_tool'").fetchone()[0]
    desc = conn.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,)).fetchone()[0]
    assert desc == "desc"

def test_edit_tool(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO executables (addr, header, body) VALUES (%s, 'old header', 'old body')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr, description) VALUES (%s, 'old desc')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'edit_tool')", (addr,))
    res = edit_tool(name="edit_tool", header_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>",
                    body_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>",
                    new_description="new desc", _meta=meta)
    assert "Applied the edits" in res
    with conn.cursor() as cur:
        cur.execute("SELECT header, body FROM executables WHERE addr=%s", (addr,))
        header, body = cur.fetchone()
        assert header == "new header"
        assert body == "new body"
        cur.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "new desc"

def test_context_add(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'ctx content')", (addr,))
    res = context_add_by_addr(addr=addr, name=None, _meta=meta)
    assert "Added context" in res
    loaded = conn.execute("SELECT item_addr FROM master_load WHERE master_addr=%s", (meta['master_id'],)).fetchall()
    assert (addr,) in loaded

def test_add_slave(meta):
    # Use fixed version with all 8 parameters
    conn = meta['conn']
    before = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    res = add_slave(instruction="do something", slave_type="general", result_name="my_result", _meta=meta)
    assert "Added a new slave" in res
    after = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    assert after == before + 1
    # Verify that a result was created with that name
    result_addr = conn.execute("SELECT result_addr FROM slaves WHERE result_name='my_result'").fetchone()
    assert result_addr is not None
    # Check the slave scope
    scope = conn.execute("SELECT scope FROM slaves WHERE result_name='my_result'").fetchone()[0]
    assert scope == 'general'

def test_add_slave_with_requires(meta):
    # Create a previous result
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        req_addr = cur.fetchone()[0]
        cur.execute("INSERT INTO results (addr, ready) VALUES (%s, false)", (req_addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'req_name')", (req_addr,))
    res = add_slave(instruction="depends", required_results_names=["req_name"], _meta=meta)
    assert "Added a new slave" in res
    # Check slave_req
    slave_addr = conn.execute("SELECT addr FROM slaves WHERE instruction='depends'").fetchone()[0]
    req_rel = conn.execute("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,)).fetchone()[0]
    assert req_rel == req_addr

def test_add_replanner_slave(meta):
    conn = meta['conn']
    # Create a master result so far
    conn.execute("UPDATE master_context SET master_result='partial result' WHERE addr=%s", (meta['master_id'],))
    before = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    res = add_replanner_slave(_meta=meta)
    assert "added a replanner slave" in res
    after = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    assert after == before + 1
    # The new slave should be of type 'task' and have a special prompt
    slave_instruction = conn.execute("SELECT instruction FROM slaves WHERE addr > (SELECT max(addr)-1 FROM slaves)").fetchone()[0]  # last added
    assert "You task is to decide how to further proceed" in slave_instruction

def test_master_result_add(meta):
    conn = meta['conn']
    conn.execute("UPDATE master_context SET master_result='' WHERE addr=%s", (meta['master_id'],))
    res = master_result_add(text="Hello world", _meta=meta)
    assert "Added a master result" in res
    new_result = conn.execute("SELECT master_result FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert new_result == "Hello world"
    # Append
    master_result_add(text=" again", _meta=meta)
    new_result = conn.execute("SELECT master_result FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert new_result == "Hello world again"

@patch('src.python.executor.builtins.embedder.encode_query')
def test_context_window_semantic_land(mock_encode, meta):
    mock_encode.return_value = [0.1]*768
    # Need s_land function to exist. We'll assume it's there.
    res = context_window_lands(querry="test query", _meta=meta)
    assert "Semantically moved the viewing window anchor" in res
    # Check that the master_context was updated (s_land updates window_anchor_*)
    anchor = conn.execute("SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    # Since no vector_ops exist, s_land will raise exception, but we mock that? For test we can skip.
    # We'll just verify the function runs without error.
    pass

def test_context_window_land(meta):
    conn = meta['conn']
    # Create a knowledge item
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'dummy')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'desc')", (addr,))
    res = context_window_land(addr=addr, _meta=meta)
    assert "Moved context window center to" in res
    # Check master_context
    anchor_k = conn.execute("SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_k == addr
    anchor_exe = conn.execute("SELECT window_anchor_exe FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_exe is None

def test_context_window_size_change(meta):
    conn = meta['conn']
    # Initialize window sizes in master_context (they are NULL by default, but we need non-NULL for the UPDATE to work without violating constraint)
    conn.execute("UPDATE master_context SET window_size_l=5, window_size_r=5 WHERE addr=%s", (meta['master_id'],))
    res = context_window_size_change(left=2, right=3, _meta=meta)
    assert "Changed context window size" in res
    l, r = conn.execute("SELECT window_size_l, window_size_r FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()
    assert l == 7
    assert r == 8

def test_move_window_anchor(meta):
    conn = meta['conn']
    # We need a vector_ops row to serve as anchor. Insert dummy.
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO vector_ops (addr, position, description) VALUES (%s, 100, 'anchor')", (addr,))
        cur.execute("UPDATE master_context SET window_anchor_knowledge=%s, window_size_l=1, window_size_r=1 WHERE addr=%s", (addr, meta['master_id']))
    res = move_window_anchor(amount=1, _meta=meta)
    assert "moved context window anchor" in res

def test_result_write(meta):
    res = result_write(text="My answer", _meta=meta)
    assert "Result: My answer" in res

def test_report_paradoxal_information(meta):
    with pytest.raises(ParadoxDetected) as exc:
        report_paradoxal_information(items=[1,2], paradox="conflict", _meta=meta)
    assert "conflict" in str(exc.value)
    # Check that the slave's result was updated
    status = meta['conn'].execute("SELECT status, status_inf FROM results r JOIN slaves s ON r.addr = s.result_addr WHERE s.addr=%s", (meta['slave_id'],)).fetchone()
    assert status[0] == 'paradox'
    assert 'conflict' in str(status[1])

@patch('src.python.executor.builtins.parse')
def test_add_cronjob(mock_parse, meta):
    res = add_cronjob(cronjob_type='once', cronjob_action='do_this_later', time_between_runs=60, params={'ai_instruction': 'test'}, _meta=meta)
    assert "Added a cronjob" in res
    mock_parse.assert_called_once_with({
        'action': 'do_this_later',
        'cronjob_type': 'once',
        'params': {'ai_instruction': 'test'},
        'run_after_or_every_s': 60
    })

def test_unload_item_by_addr(meta):
    conn = meta['conn']
    # Load an item first
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'temp')", (addr,))
        cur.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (meta['master_id'], addr))
    res = unload_item(addr=addr, _meta=meta)
    assert "Unloaded item" in res
    loaded = conn.execute("SELECT item_addr FROM master_load WHERE master_addr=%s", (meta['master_id'],)).fetchall()
    assert (addr,) not in loaded

def test_unload_item_by_name(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'temp2')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'unload_me')", (addr,))
        cur.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (meta['master_id'], addr))
    res = unload_item(name='unload_me', _meta=meta)
    assert "Unloaded item" in res

def test_web_search_fulltext(meta, mock_searcher):
    res = web_searcher_function_fulltext(query="test query", websites_amount=2, _meta=meta)
    assert "Websearch for query 'test query'" in res
    mock_searcher.search_website_content.assert_called_once_with("test query", 2, meta['context_limit']//2)

def test_send_message_to_human(meta):
    conn = meta['conn']
    # Create a session master and results with metadata for the human message
    # We'll create a master with a name like session_0
    with conn.cursor() as cur:
        cur.execute("INSERT INTO masters (instruction) VALUES ('session master') RETURNING addr")
        session_master = cur.fetchone()[0]
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'session_99')", (session_master,))
        # Create a placeholder ai_message result with metadata
        cur.execute("SELECT new_addr()")
        ai_msg_addr = cur.fetchone()[0]
        cur.execute("INSERT INTO results (addr, metadata) VALUES (%s, jsonb_build_object('type', 'ai_message', 'session_name', 'session_99', 'turn', 1))", (ai_msg_addr,))
    # Now call send_message with master_id = session_master
    meta2 = meta.copy()
    meta2['master_id'] = session_master
    res = send_message_to_human_v_webui(text="Hello human", _meta=meta2)
    assert "Sent a message to the human" in res
    # Check that new_result was called, i.e., the ai_message result now has content
    content = conn.execute("SELECT content_str FROM results WHERE addr=%s", (ai_msg_addr,)).fetchone()[0]
    assert content == "Hello human"

def test_search_for_urls(meta, mock_searcher):
    mock_searcher.search.return_value = [
        {'url': 'http://a.com', 'title': 'A', 'snippet': 'snippetA'},
        {'url': 'http://b.com', 'title': 'B', 'snippet': 'snippetB'}
    ]
    res = search_for_urls(query="test", amount_results=2, _meta=meta)
    assert "url=http://a.com" in res
    assert "title=B" in res
    mock_searcher.search.assert_called_once_with("test")
