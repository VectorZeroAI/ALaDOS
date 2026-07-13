# tests/test_builtins.py
import pytest
import psycopg
import time
from unittest.mock import Mock, patch

# Adjust import path to match your project structure
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
    """Connection to test database. Autocommit off – tests control transactions."""
    conn = psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="alados_test"
    )
    conn.autocommit = False # FIXME : This is no longer the same as true conn factory, so this needs to be fixed to stay in check with conn_factory.
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
    meta_obj = {
        'conn': test_conn,
        'master_id': master_addr,
        'slave_id': slave_addr,
        '_embedder_queue': Uqueue(),
        'context_limit': 10000
    }
    yield meta_obj
    test_conn.rollback()  # discard all changes after each test


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


# ---------- Unique name generator ----------
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
    # Create a knowledge item using the builtin (so vector_ops is set correctly)
    name = unique_name("edit_addr")
    k_create(content="old content", description="old desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    res = k_edit(addr=addr, content_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>", _meta=meta)
    assert "Edited the knowledge item" in res
    with conn.cursor() as cur:
        cur.execute("SELECT content FROM knowledge WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "new content"
        # description unchanged
        cur.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,))
        assert cur.fetchone()[0] == "old desc"


def test_k_edit_by_name(meta):
    conn = meta['conn']
    name = unique_name("edit_name")
    k_create(content="foo", description="bar", name=name, _meta=meta)
    res = k_edit(name=name, description_change="<SEARCH>bar</SEARCH><REPLACE>baz</REPLACE>", _meta=meta)
    assert "Edited the knowledge item" in res
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    desc = conn.execute("SELECT description FROM vector_ops WHERE addr=%s", (addr,)).fetchone()[0]
    assert desc == "baz"


def test_k_read_by_addr(meta):
    conn = meta['conn']
    name = unique_name("read_addr")
    k_create(content="secret", description="desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    res = k_read(addr=addr, _meta=meta)
    assert "secret" in res


def test_k_read_by_name(meta):
    name = unique_name("read_name")
    k_create(content="secret2", description="desc", name=name, _meta=meta)
    res = k_read(name=name, _meta=meta)
    assert "secret2" in res


def test_execute_tool(meta, mock_subprocess):
    tool_name = unique_name("test_tool")
    create_tool(description="desc", header="test header", body="print('hello')", name=tool_name, _meta=meta)
    res = execute_tool_builtin_func(name=tool_name, kwargs={"x": 1}, _meta=meta)
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
    tool_name = unique_name("edit_tool")
    create_tool(description="old desc", header="old header", body="old body", name=tool_name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (tool_name,)).fetchone()[0]
    res = edit_tool(name=tool_name,
                    header_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>",
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
    name = unique_name("ctx_item")
    k_create(content="ctx content", description="desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    res = context_add_by_addr(addr=addr, name=None, _meta=meta)
    assert "Added context" in res
    loaded = conn.execute("SELECT item_addr FROM master_load WHERE master_addr=%s", (meta['master_id'],)).fetchall()
    assert (addr,) in loaded


def test_add_slave(meta):
    conn = meta['conn']
    before = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    result_name = unique_name("slave_result")
    res = add_slave(instruction="do something", slave_type="general", result_name=result_name, _meta=meta)
    assert "Added a new slave" in res
    after = conn.execute("SELECT count(*) FROM slaves WHERE master_addr=%s", (meta['master_id'],)).fetchone()[0]
    assert after == before + 1
    result_addr = conn.execute("SELECT result_addr FROM slaves WHERE result_name=%s", (result_name,)).fetchone()
    assert result_addr is not None
    scope = conn.execute("SELECT scope FROM slaves WHERE result_name=%s", (result_name,)).fetchone()[0]
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
    # Add a slave that requires it
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
    # Check that the new slave has the expected prompt (last inserted slave)
    cur = conn.execute("SELECT instruction FROM slaves WHERE master_addr=%s ORDER BY addr DESC LIMIT 1", (meta['master_id'],))
    slave_instruction = cur.fetchone()[0]
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
    # Insert a dummy knowledge with a vector so s_land has something
    conn = meta['conn']
    name = unique_name("semantic_dummy")
    k_create(content="dummy", description="dummy desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    # Update vector_ops to have a non‑null emb (the trigger will set position)
    conn.execute("UPDATE vector_ops SET emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s", (addr,))
    # Now semantic land should find it
    res = context_window_lands(querry="test query", _meta=meta)
    assert "Semantically moved the viewing window anchor" in res


def test_context_window_land(meta):
    conn = meta['conn']
    name = unique_name("window_anchor")
    k_create(content="anchor", description="anchor desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    res = context_window_land(addr=addr, _meta=meta)
    assert "Moved context window center to" in res
    anchor_k = conn.execute("SELECT window_anchor_knowledge FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_k == addr
    anchor_exe = conn.execute("SELECT window_anchor_exe FROM master_context WHERE addr=%s", (meta['master_id'],)).fetchone()[0]
    assert anchor_exe is None


def test_context_window_size_change(meta):
    conn = meta['conn']
    # First set an anchor and valid sizes to satisfy the constraint
    name = unique_name("size_anchor")
    k_create(content="anchor", description="desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    conn.execute("""
        UPDATE master_context 
        SET window_anchor_knowledge = %s, window_size_l = 5, window_size_r = 5 
        WHERE addr = %s
    """, (addr, meta['master_id']))
    res = context_window_size_change(left=2, right=3, _meta=meta)
    assert "Changed context window size" in res
    l, r = conn.execute("SELECT window_size_l, window_size_r FROM master_context WHERE addr=%s", (meta.master_id,)).fetchone()
    assert l == 7
    assert r == 8


def test_move_window_anchor(meta):
    conn = meta['conn']
    # Create a dummy vector_ops row with a known position
    name = unique_name("move_anchor")
    k_create(content="anchor knowledge", description="anchor desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]

    conn.execute("UPDATE vector_ops SET position = 100, emb = array_fill(0.0, ARRAY[768])::vector(768) WHERE addr = %s", (addr,))

    name2 = unique_name("move_anchor")
    k_create(content="anchor knowledge", description="anchor desc", name=name2, _meta=meta)
    addr2 = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]

    conn.execute("UPDATE vector_ops SET position = 124, emb = array_fill(0.2, ARRAY[768])::vector(768) WHERE addr = %s", (addr2,))

    conn.execute("""
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
        report_paradoxal_information(items=[1, 2], paradox="conflict", _meta=meta)
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
    name = unique_name("unload_addr")
    k_create(content="temp", description="desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    # Load it into context
    conn.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (meta['master_id'], addr))
    res = unload_item(addr=addr, _meta=meta)
    assert "Unloaded item" in res
    loaded = conn.execute("SELECT item_addr FROM master_load WHERE master_addr=%s", (meta['master_id'],)).fetchall()
    assert (addr,) not in loaded


def test_unload_item_by_name(meta):
    conn = meta['conn']
    name = unique_name("unload_name")
    k_create(content="temp2", description="desc", name=name, _meta=meta)
    addr = conn.execute("SELECT addr FROM names WHERE name=%s", (name,)).fetchone()[0]
    conn.execute("INSERT INTO master_load (master_addr, item_addr) VALUES (%s, %s)", (meta['master_id'], addr))
    res = unload_item(name=name, _meta=meta)
    assert "Unloaded item" in res


def test_web_search_fulltext(meta, mock_searcher):
    res = web_searcher_function_fulltext(query="test query", websites_amount=2, _meta=meta)
    assert "Websearch for query 'test query'" in res
    mock_searcher.search_website_content.assert_called_once_with("test query", 2, meta['context_limit'] // 2)


def test_send_message_to_human(meta):
    conn = meta['conn']
    session_name = unique_name("session_test")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO masters (instruction) VALUES ('session master') RETURNING addr")
        session_master = cur.fetchone()[0]
        cur.execute("INSERT INTO names (addr, name) VALUES (%s, %s)", (session_master, session_name))
        # Create a placeholder ai_message result with metadata; cast the parameter
        cur.execute("SELECT new_addr()")
        ai_msg_addr = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO results (addr, metadata)
            VALUES (%s, jsonb_build_object('type', 'ai_message', 'session_name', %s::text, 'turn', 1))
        """, (ai_msg_addr, session_name))
    # Now call send_message with master_id = session_master
    meta2 = meta.copy()
    meta2['master_id'] = session_master
    res = send_message_to_human_v_webui(text="Hello human", _meta=meta2)
    assert "Sent a message to the human" in res
    # Check that the ai_message result now has content
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
