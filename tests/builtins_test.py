# tests/test_builtins.py
import pytest
import psycopg
import time
from unittest.mock import Mock, patch

# Import your fixed builtins
from src.python.executor.builtins import (
    k_create, k_edit, k_read, execute_tool_builtin_func, create_tool, edit_tool,
    context_add_by_addr, add_slave, add_replanner_slave, master_result_add,
    context_window_lands, context_window_land, context_window_size_change,
    move_window_anchor, result_write, report_paradoxal_information, add_cronjob,
    unload_item, web_searcher_function_fulltext, send_message_to_human_v_webui,
    search_for_urls
)
from src.python.executor.types import _ExecToolMetaData
from src.python.executor.exceptions import ParadoxDetected
from src.python.utils.uqueue import Uqueue


# ---------- Fixtures ----------
@pytest.fixture(scope="session")
def test_conn():
    """Connection to test database. Autocommit off – tests control commits."""
    conn = psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="alados_test"
    )
    conn.autocommit = False
    yield conn
    conn.close()


@pytest.fixture
def meta(test_conn):
    """Create a fresh master and slave for each test, rolled back at the end."""
    with test_conn.cursor() as cur:
        cur.execute("INSERT INTO masters (instruction) VALUES ('test master') RETURNING addr")
        master_addr = cur.fetchone()[0]
        # Create a slave for this master so slave_id exists
        cur.execute("""
            SELECT new_slave(
                p_master_addr := %s,
                p_instruction := 'test slave',
                p_name := NULL,
                p_requires := NULL,
                p_result_addr := NULL,
                p_result_name := NULL,
                p_result_metadata := NULL,
                p_slave_scope := 'general'
            )
        """, (master_addr,))
        slave_addr = cur.fetchone()[0]
        # Ensure the slave's result exists
        cur.execute("SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,))
        result_addr = cur.fetchone()[0]
    # Return metadata, but keep the transaction open
    meta_obj = {
        'conn': test_conn,
        'master_id': master_addr,
        'slave_id': slave_addr,
        '_embedder_queue': Uqueue(),
        'context_limit': 10000
    }
    yield meta_obj
    # Rollback after each test
    test_conn.rollback()


# ---------- Mocks ----------
@pytest.fixture(autouse=True)
def mock_embedder():
    with patch('src.python.executor.builtins.embedder') as mock_emb:
        mock_emb.encode_query.return_value = [0.0] * 768
        yield mock_emb


@pytest.fixture(autouse=True)
def mock_searcher():
    with patch('src.python.executor.builtins.searcher_obj') as mock:
        mock.search.return_value = []
        mock.search_website_content.return_value = "Mocked search result"
        yield mock


@pytest.fixture(autouse=True)
def mock_subprocess():
    with patch('subprocess.run') as mock_run:
        mock_run.return_value = Mock(stdout="mocked output", stderr="")
        yield mock_run


# ---------- Helper to get a fresh name ----------
_name_counter = 0
def unique_name(base: str) -> str:
    global _name_counter
    _name_counter += 1
    return f"{base}_{_name_counter}_{int(time.time())}"


# ---------- Tests ----------
def test_k_create(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM knowledge")
        before = cur.fetchone()[0]
    name = unique_name("test_k")
    result = k_create(content="test content", description="test desc", name=name, _meta=meta)
    assert f"knowledge entry {name}@" in result
    with conn.cursor() as cur:
        cur.execute("SELECT content, description FROM knowledge k JOIN vector_ops v ON k.addr = v.addr_k WHERE k.addr = (SELECT addr FROM names WHERE name=%s)", (name,))
        content, desc = cur.fetchone()
        assert content == "test content"
        assert desc == "test desc"
        cur.execute("SELECT count(*) FROM knowledge")
        assert cur.fetchone()[0] == before + 1


def test_k_edit_by_addr(meta):
    conn = meta['conn']
    # Create item
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'old content')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'old desc')", (addr,))
    res = k_edit(addr=addr, content_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>", _meta=meta)
    assert "Edited the knowledge item" in res
    with conn.cursor() as cur:
        cur.execute("SELECT content FROM knowledge WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "new content"
        cur.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "old desc"


def test_k_edit_by_name(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'foo')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'bar')", (addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'test_name_edit')", (addr,))
    res = k_edit(name="test_name_edit", description_change="<SEARCH>bar</SEARCH><REPLACE>baz</REPLACE>", _meta=meta)
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
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, 'test_read_name')", (addr,))
    res = k_read(name="test_read_name", _meta=meta)
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


def test_create_tool(meta):
    conn = meta['conn']
    before = conn.execute("SELECT count(*) FROM executables").fetchone()[0]
    name = unique_name("my_tool")
    res = create_tool(description="desc", header="header", body="body", name=name, _meta=meta)
    assert f"Created tool {name}@" in res
    after = conn.execute("SELECT count(*) FROM executables").fetchone()[0]
    assert after == before + 1
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
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
    conn = meta['conn']
    before = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    res = add_slave(instruction="do something", slave_type="general", result_name="my_result", _meta=meta)
    assert "Added a new slave" in res
    after = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    assert after == before + 1
    result_addr = conn.execute("SELECT result_addr FROM slaves WHERE result_name='my_result'").fetchone()
    assert result_addr is not None
    scope = conn.execute("SELECT scope FROM slaves WHERE result_name='my_result'").fetchone()[0]
    assert scope == 'general'


def test_add_slave_with_requires(meta):
    conn = meta['conn']
    # Create a previous result with unique name
    req_name = unique_name("req_name")
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        req_addr = cur.fetchone()[0]
        cur.execute("INSERT INTO results (addr, ready) VALUES (%s, false)", (req_addr,))
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (req_addr, req_name))
    res = add_slave(instruction="depends", required_results_names=[req_name], _meta=meta)
    assert "Added a new slave" in res
    slave_addr = conn.execute("SELECT addr FROM slaves WHERE instruction='depends'").fetchone()[0]
    req_rel = conn.execute("SELECT req_addr FROM slave_req WHERE slave_addr=%s", (slave_addr,)).fetchone()[0]
    assert req_rel == req_addr


def test_add_replanner_slave(meta):
    conn = meta['conn']
    conn.execute("UPDATE master_context SET master_result='partial result' WHERE addr=%s", (meta['master_id'],))
    before = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    res = add_replanner_slave(_meta=meta)
    assert "added a replanner slave" in res
    after = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    assert after == before + 1
    slave_instruction = conn.execute("SELECT instruction FROM slaves WHERE addr > (SELECT max(addr)-1 FROM slaves)").fetchone()[0]
    assert "You task is to decide how to further proceed" in slave_instruction


def test_master_result_add(meta):
    conn = meta['conn']
    conn.execute("UPDATE master_context SET master_result='' WHERE addr=%s", (meta['master_id'],))
    res = master_result_add(text="Hello world", _meta=meta)
    assert "Added a master result" in res
    new_result = conn.execute("SELECT master_result FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert new_result == "Hello world"
    master_result_add(text=" again", _meta=meta)
    new_result = conn.execute("SELECT master_result FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert new_result == "Hello world again"


def test_context_window_semantic_land(meta):
    # Insert dummy vector_ops items so s_land has something to anchor on
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'dummy')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description, emb) VALUES (%s, 'dummy desc', array_fill(0.0, ARRAY[768])::vector(768))", (addr,))
    # Now semantic land should find something
    res = context_window_lands(querry="test query", _meta=meta)
    assert "Semantically moved the viewing window anchor" in res


def test_context_window_land(meta):
    conn = meta['conn']
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'dummy')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'desc')", (addr,))
    res = context_window_land(addr=addr, _meta=meta)
    assert "Moved context window center to" in res
    anchor_k = conn.execute("SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_k == addr
    anchor_exe = conn.execute("SELECT window_anchor_exe FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_exe is None


def test_context_window_size_change(meta):
    conn = meta['conn']
    # First set an anchor and valid sizes to satisfy constraint
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'anchor')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description) VALUES (%s, 'anchor desc')", (addr,))
        cur.execute("""
            UPDATE master_context 
            SET window_anchor_knowledge = %s, window_size_l = 5, window_size_r = 5 
            WHERE addr = %s
        """, (addr, meta['master_id']))
    res = context_window_size_change(left=2, right=3, _meta=meta)
    assert "Changed context window size" in res
    l, r = conn.execute("SELECT window_size_l, window_size_r FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()
    assert l == 7
    assert r == 8


def test_move_window_anchor(meta):
    conn = meta['conn']
    # Create a dummy vector_ops row (addr is generated)
    with conn.cursor() as cur:
        cur.execute("SELECT new_addr()")
        addr = cur.fetchone()[0]
        # Insert into executables or knowledge first to get a valid addr for vector_ops
        cur.execute("INSERT INTO knowledge (addr, content) VALUES (%s, 'anchor knowledge')", (addr,))
        cur.execute("INSERT INTO vector_ops (addr_k, description, position) VALUES (%s, 'anchor desc', 100)", (addr,))
        # Set anchor and window sizes in master_context
        cur.execute("""
            UPDATE master_context 
            SET window_anchor_knowledge = %s, window_size_l = 1, window_size_r = 1 
            WHERE addr = %s
        """, (addr, meta['master_id']))
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
    status = meta['conn'].execute(
        "SELECT status, status_inf FROM results r JOIN slaves s ON r.addr = s.result_addr WHERE s.addr=%s",
        (meta['slave_id'],)
    ).fetchone()
    assert status[0] == 'paradox'
    assert 'conflict' in str(status[1])


@patch('src.python.executor.builtins.parse')
def test_add_cronjob(mock_parse, meta):
    res = add_cronjob(cronjob_type='once', cronjob_action='do_this_later', time_between_runs=60,
                      params={'ai_instruction': 'test'}, _meta=meta)
    assert "Added a cronjob" in res
    mock_parse.assert_called_once_with({
        'action': 'do_this_later',
        'cronjob_type': 'once',
        'params': {'ai_instruction': 'test'},
        'run_after_or_every_s': 60
    })


def test_unload_item_by_addr(meta):
    conn = meta['conn']
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
        name = unique_name("unload_me")
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (addr, name))
        cur.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (meta['master_id'], addr))
    res = unload_item(name=name, _meta=meta)
    assert "Unloaded item" in res


def test_web_search_fulltext(meta, mock_searcher):
    res = web_searcher_function_fulltext(query="test query", websites_amount=2, _meta=meta)
    assert "Websearch for query 'test query'" in res
    mock_searcher.search_website_content.assert_called_once_with("test query", 2, meta['context_limit']//2)


def test_send_message_to_human(meta):
    conn = meta['conn']
    session_name = unique_name("session_test")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO masters (instruction) VALUES ('session master') RETURNING addr")
        session_master = cur.fetchone()[0]
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (session_master, session_name))
        # Create a placeholder ai_message result with metadata
        cur.execute("SELECT new_addr()")
        ai_msg_addr = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO results (addr, metadata)
            VALUES (%s, jsonb_build_object('type', 'ai_message', 'session_name', %s, 'turn', 1))
        """, (ai_msg_addr, session_name))
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
