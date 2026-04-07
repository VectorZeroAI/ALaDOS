#!/usr/bin/env python3
"""
Tests for ALaDOS context resolution — src/python/scheduler/goal_stack/context.py

Uses a real PostgreSQL test DB seeded with the actual schema from src/sql/.
Mock level: conn_factory() is monkeypatched to point at the test DB.
Everything else runs for real.

Setup (one-time):
    createdb aladostest
    psql aladostest < src/sql/001_db_schema.sql
    psql aladostest < src/sql/002_functions.sql
    # 003_notifiers.sql has syntax errors — skip it for tests, it's not needed here.

Run from repo root:
    TEST_DB=aladostest pytest tests/context_test.py -v

Bugs are marked xfail. Fix the bug → xfail becomes xpass → remove the mark.
"""

import os
import pytest
import psycopg

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

TEST_DSN = dict(
    host="127.0.0.1",
    port=5432,
    dbname=os.environ.get("TEST_DB", "test"),
    user=os.environ.get("TEST_DB_USER", "u0_a453"),
)


def get_test_conn() -> psycopg.Connection:
    conn = psycopg.connect(**TEST_DSN) # pyright: ignore
    conn.autocommit = True
    return conn


# ---------------------------------------------------------------------------
# Patch conn_factory before any import of context.py
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_conn_factory(monkeypatch):
    """
    Replace python.utils.conn_factory.conn_factory everywhere so that
    context.py hits the test DB instead of the production one.
    """
    import sys, types as _types

    src = f"""
import psycopg
def conn_factory():
    conn = psycopg.connect(
        host='{TEST_DSN["host"]}', port={TEST_DSN["port"]},
        dbname='{TEST_DSN["dbname"]}', user='{TEST_DSN["user"]}'
    )
    conn.autocommit = True
    return conn
"""
    fake = _types.ModuleType("python.utils.conn_factory")
    exec(src, fake.__dict__)
    monkeypatch.setitem(sys.modules, "python.utils.conn_factory", fake)

    # Also patch the already-bound reference inside context if it was imported earlier
    try:
        from ..src.python.sceduler.goal_stack import context as ctx
        monkeypatch.setattr(ctx, "conn_factory", fake.conn_factory)
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# Schema teardown / bring-up
# ---------------------------------------------------------------------------

# Drop order respects FK dependencies.
TEARDOWN = """
DROP VIEW  IF EXISTS addrs_tables   CASCADE;
DROP VIEW  IF EXISTS viewing_window CASCADE;
DROP TABLE IF EXISTS slave_req      CASCADE;
DROP TABLE IF EXISTS master_load    CASCADE;
DROP TABLE IF EXISTS master_context CASCADE;
DROP TABLE IF EXISTS ownership      CASCADE;
DROP TABLE IF EXISTS slaves         CASCADE;
DROP TABLE IF EXISTS results        CASCADE;
DROP TABLE IF EXISTS logs           CASCADE;
DROP TABLE IF EXISTS executables    CASCADE;
DROP TABLE IF EXISTS knowledge      CASCADE;
DROP TABLE IF EXISTS masters        CASCADE;
DROP TABLE IF EXISTS names          CASCADE;
DROP TABLE IF EXISTS addrs          CASCADE;
DROP SEQUENCE IF EXISTS global_next_id    CASCADE;
DROP SEQUENCE IF EXISTS update_counter_window CASCADE;
DROP FUNCTION IF EXISTS new_addr()  CASCADE;
DROP FUNCTION IF EXISTS new_slave   CASCADE;
DROP FUNCTION IF EXISTS resolve_name CASCADE;
DROP FUNCTION IF EXISTS new_result  CASCADE;
"""


@pytest.fixture(scope="session")
def db() -> psycopg.Connection:
    """Session-scoped connection. Tears down and recreates schema once."""
    conn = get_test_conn()
    conn.execute(TEARDOWN)

    sql_dir = os.path.join(os.path.dirname(__file__), "..", "src", "sql")
    for fname in ("001_db_schema.sql", "002_functions.sql"):
        path = os.path.join(sql_dir, fname)
        with open(path) as f:
            conn.execute(f.read())

    yield conn
    conn.execute(TEARDOWN)
    conn.close()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def new_addr(conn: psycopg.Connection) -> int:
    """Use the DB's own new_addr() function, matching production behaviour."""
    return conn.execute("SELECT new_addr()").fetchone()[0]


def insert_knowledge(conn: psycopg.Connection, name: str, content: str,
                     description: str, position: int = 10) -> int:
    addr = conn.execute(
        "INSERT INTO knowledge (content, description, position) VALUES ($1,$2,$3) RETURNING addr",
        (content, description, position)
    ).fetchone()[0]
    conn.execute("INSERT INTO names (addr, name) VALUES ($1,$2)", (addr, name))
    return addr


def insert_executable(conn: psycopg.Connection, name: str, header: str,
                       body: str, description: str, position: int = 20) -> int:
    addr = conn.execute(
        "INSERT INTO executables (header, body, description, position) VALUES ($1,$2,$3,$4) RETURNING addr",
        (header, body, description, position)
    ).fetchone()[0]
    conn.execute("INSERT INTO names (addr, name) VALUES ($1,$2)", (addr, name))
    return addr


def insert_master(conn: psycopg.Connection) -> int:
    return conn.execute(
        "INSERT INTO masters DEFAULT VALUES RETURNING addr"
    ).fetchone()[0]


def insert_result(conn: psycopg.Connection, content: str = "", ready: bool = False) -> int:
    return conn.execute(
        "INSERT INTO results (content_str, ready) VALUES ($1,$2) RETURNING addr",
        (content, ready)
    ).fetchone()[0]


def insert_slave(conn: psycopg.Connection, master_addr: int, name: str,
                 instruction: str, result_addr: int,
                 result_name: str, requires: list[int] | None = None) -> int:
    """Uses the new_slave() DB function, same as production."""
    requires = requires or []
    return conn.execute(
        "SELECT new_slave($1,$2,$3,$4,$5,$6)",
        (master_addr, name, instruction, requires, result_addr, result_name)
    ).fetchone()[0]


def insert_log(conn: psycopg.Connection, name: str, action: str,
               created_by: str) -> int:
    addr = conn.execute(
        "INSERT INTO logs (action, created_by) VALUES ($1,$2) RETURNING addr",
        (action, created_by)
    ).fetchone()[0]
    conn.execute("INSERT INTO names (addr, name) VALUES ($1,$2)", (addr, name))
    return addr


# ---------------------------------------------------------------------------
# Shared seed fixture (function-scoped so each test class gets its own rows)
# ---------------------------------------------------------------------------

@pytest.fixture()
def seed(db: psycopg.Connection) -> dict:
    k_addr = insert_knowledge(db, "TestKnowledge", "This is test knowledge content.",
                               "knowledge description", position=10)
    e_addr = insert_executable(db, "TestExecutable", "def foo():", "    return 42",
                                "executable description", position=20)
    r_addr = insert_result(db, "result content here", ready=True)
    m_addr = insert_master(db)
    s_addr = insert_slave(db, m_addr, "TestSlave", "slave instruction",
                          r_addr, "my_result", requires=[])

    db.execute(
        """INSERT INTO master_context
               (addr, window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l)
           VALUES ($1, NULL, $2, 5, 5)""",
        (m_addr, k_addr)
    )
    db.execute(
        "INSERT INTO master_load (master_addr, item_addr) VALUES ($1,$2)", (m_addr, k_addr)
    )
    db.execute(
        "INSERT INTO master_load (master_addr, item_addr) VALUES ($1,$2)", (m_addr, e_addr)
    )

    return dict(
        k_addr=k_addr, e_addr=e_addr, r_addr=r_addr,
        m_addr=m_addr, s_addr=s_addr,
        slave_obj=dict(
            addr=s_addr,
            instruction="slave instruction",
            master_addr=m_addr,
            result_name="my_result",
        ),
    )


# ---------------------------------------------------------------------------
# _resolve_knowledge_item
# ---------------------------------------------------------------------------

class TestResolveKnowledgeItem:
    def test_header_format(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _resolve_knowledge_item
        result = _resolve_knowledge_item(seed["k_addr"], db)
        assert isinstance(result, str)
        # Expected header line: "TestKnowledge@<addr>@knowledge"
        assert f"TestKnowledge@{seed['k_addr']}@knowledge" in result

    def test_content_present(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _resolve_knowledge_item
        result = _resolve_knowledge_item(seed["k_addr"], db)
        assert "This is test knowledge content." in result

    def test_nonexistent_addr_raises(self, db):
        from ..src.python.sceduler.goal_stack.context import _resolve_knowledge_item
        with pytest.raises(AssertionError):
            _resolve_knowledge_item(999999999, db)


# ---------------------------------------------------------------------------
# _executables_item_resolve
# ---------------------------------------------------------------------------

class TestExecutablesItemResolve:
    def test_header_format(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _executables_item_resolve
        result = _executables_item_resolve(seed["e_addr"], db)
        assert f"TestExecutable@{seed['e_addr']}@executable" in result

    def test_header_and_body_labels(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _executables_item_resolve
        result = _executables_item_resolve(seed["e_addr"], db)
        assert "header: def foo():" in result
        assert "body:     return 42" in result

    def test_nonexistent_raises(self, db):
        from ..src.python.sceduler.goal_stack.context import _executables_item_resolve
        with pytest.raises(AssertionError):
            _executables_item_resolve(999999999, db)


# ---------------------------------------------------------------------------
# _result_item_resolve
# ---------------------------------------------------------------------------

class TestResultItemResolve:
    def test_format(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _result_item_resolve
        result = _result_item_resolve(seed["r_addr"], db)
        # result_name comes from the slave row joined via result_addr
        assert "my_result" in result
        assert str(seed["r_addr"]) in result

    def test_content_present(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _result_item_resolve
        result = _result_item_resolve(seed["r_addr"], db)
        assert "result content here" in result

    def test_ready_flag(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _result_item_resolve
        result = _result_item_resolve(seed["r_addr"], db)
        assert "True" in result


# ---------------------------------------------------------------------------
# _slaves_item_resolve
# ---------------------------------------------------------------------------

class TestSlavesItemResolve:
    def test_format(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _slaves_item_resolve
        result = _slaves_item_resolve(seed["s_addr"], db)
        assert f"TestSlave@{seed['s_addr']}@slave_goal" in result

    def test_fields_present(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _slaves_item_resolve
        result = _slaves_item_resolve(seed["s_addr"], db)
        assert "slave instruction" in result
        assert str(seed["m_addr"]) in result
        assert "my_result" in result
        assert str(seed["r_addr"]) in result

    def test_nonexistent_raises(self, db):
        from ..src.python.sceduler.goal_stack.context import _slaves_item_resolve
        with pytest.raises(AssertionError):
            _slaves_item_resolve(999999999, db)


# ---------------------------------------------------------------------------
# _masters_item_resolve
# BUG: "\n".join(result_str, *slave_str_list) — str.join() takes one iterable.
# Fix: "\n".join([result_str, *slave_str_list])
# ---------------------------------------------------------------------------

class TestMastersItemResolve:
    @pytest.mark.xfail(
        reason="str.join() called with *args instead of a single iterable — TypeError"
    )
    def test_format(self, db, seed):
        from ..src.python.sceduler.goal_stack.context import _masters_item_resolve
        result = _masters_item_resolve(seed["m_addr"], db)
        assert "master_goal" in result
        assert "slave instruction" in result
        assert str(seed["r_addr"]) in result

    @pytest.mark.xfail(reason="same str.join bug")
    def test_no_slaves(self, db):
        """A master with zero slaves should still return a valid string."""
        m_addr = insert_master(db)
        db.execute(
            "INSERT INTO master_context (addr) VALUES ($1)", (m_addr,)
        )
        from ..src.python.sceduler.goal_stack.context import _masters_item_resolve
        result = _masters_item_resolve(m_addr, db)
        assert "master_goal" in result


# ---------------------------------------------------------------------------
# _logs_item_resolve
# Note: logs.created_by is TEXT (not INT) per the real schema.
# ---------------------------------------------------------------------------

class TestLogsItemResolve:
    def test_format(self, db, seed):
        log_addr = insert_log(db, "TestLog", "did_something", "master_system")
        from ..src.python.sceduler.goal_stack.context import _logs_item_resolve
        result = _logs_item_resolve(log_addr, db)
        assert f"TestLog@{log_addr}@log_item" in result
        assert "did_something" in result
        assert "master_system" in result

    def test_nonexistent_raises(self, db):
        from ..src.python.sceduler.goal_stack.context import _logs_item_resolve
        with pytest.raises(AssertionError):
            _logs_item_resolve(999999999, db)


# ---------------------------------------------------------------------------
# resolve_loads
# BUG: iterates item_addrs as ints, then does addr["ref_addr"] — TypeError.
# Fix: use addr directly in the match block, not addr["ref_addr"].
# Also: addrs_tables is a VIEW (not a table with a "table" column) — column is "type".
# Fix: SELECT type FROM addrs_tables WHERE addr = $1
# ---------------------------------------------------------------------------

class TestResolveLoads:
    @pytest.mark.xfail(
        reason="Two bugs: (1) addr['ref_addr'] on an int, (2) column is 'type' not 'table' in addrs_tables view"
    )
    def test_knowledge_and_executable(self, seed):
        from ..src.python.sceduler.goal_stack.context import resolve_loads
        loads = {
            "master_addr": seed["m_addr"],
            "items_addrs": [seed["k_addr"], seed["e_addr"]],
        }
        result = resolve_loads(loads)
        assert isinstance(result, str)
        assert "TestKnowledge" in result
        assert "TestExecutable" in result

    @pytest.mark.xfail(reason="same bugs as above")
    def test_result_item(self, seed):
        from ..src.python.sceduler.goal_stack.context import resolve_loads
        loads = {
            "master_addr": seed["m_addr"],
            "items_addrs": [seed["r_addr"]],
        }
        result = resolve_loads(loads)
        assert "my_result" in result

    def test_empty_returns_empty_string(self, seed):
        """Zero items — no DB queries needed, should always return ''."""
        from ..src.python.sceduler.goal_stack.context import resolve_loads
        loads = {
            "master_addr": seed["m_addr"],
            "items_addrs": [],
        }
        result = resolve_loads(loads)
        assert result == ""

    @pytest.mark.xfail(reason="same bugs; ValueError only reachable once addr lookup is fixed")
    def test_unknown_type_raises_value_error(self, db, seed):
        """
        addrs_tables is a view — it only covers known tables.
        An addr that exists in addrs but in no known table won't appear in the
        view at all, so fetchone() returns None → should raise, not silently pass.
        """
        orphan = new_addr(db)  # exists in addrs, not in any content table
        from ..src.python.sceduler.goal_stack.context import resolve_loads
        loads = {
            "master_addr": seed["m_addr"],
            "items_addrs": [orphan],
        }
        with pytest.raises((ValueError, TypeError)):
            resolve_loads(loads)


# ---------------------------------------------------------------------------
# resolve_window
# BUG: "SELECT position FROM $1 WHERE addr = $2" — table name can't be a
# psycopg parameter. Raises ProgrammingError at runtime.
# Fix: use psycopg.sql.SQL + psycopg.sql.Identifier for the table name.
# ---------------------------------------------------------------------------

class TestResolveWindow:
    @pytest.mark.xfail(
        reason="Table name passed as $1 parameter — not valid SQL, needs psycopg.sql.Identifier"
    )
    def test_knowledge_anchor(self, seed):
        from ..src.python.sceduler.goal_stack.context import resolve_window
        window = {
            "master_addr": seed["m_addr"],
            "window_position": {
                "ref_addr": seed["k_addr"],
                "ref_table": "knowledge",
            },
            "window_size_l": 5,
            "window_size_r": 5,
        }
        result = resolve_window(window)
        assert isinstance(result, str)
        assert len(result) > 0

    @pytest.mark.xfail(reason="same Identifier bug")
    def test_executable_anchor(self, seed):
        from ..src.python.sceduler.goal_stack.context import resolve_window
        window = {
            "master_addr": seed["m_addr"],
            "window_position": {
                "ref_addr": seed["e_addr"],
                "ref_table": "executables",
            },
            "window_size_l": 5,
            "window_size_r": 5,
        }
        result = resolve_window(window)
        assert isinstance(result, str)

    @pytest.mark.xfail(reason="same Identifier bug")
    def test_items_within_range_appear(self, db, seed):
        """
        knowledge at position 10, window L=5 R=5 → range [5, 15].
        TestKnowledge (pos 10) should be in output.
        TestExecutable (pos 20) should not.
        """
        from ..src.python.sceduler.goal_stack.context import resolve_window
        window = {
            "master_addr": seed["m_addr"],
            "window_position": {
                "ref_addr": seed["k_addr"],
                "ref_table": "knowledge",
            },
            "window_size_l": 5,
            "window_size_r": 5,
        }
        result = resolve_window(window)
        assert "TestKnowledge" in result
        assert "TestExecutable" not in result

    @pytest.mark.xfail(reason="same Identifier bug")
    def test_zero_window_returns_only_anchor(self, seed):
        """L=0, R=0 → only the anchor item's position is included."""
        from ..src.python.sceduler.goal_stack.context import resolve_window
        window = {
            "master_addr": seed["m_addr"],
            "window_position": {
                "ref_addr": seed["k_addr"],
                "ref_table": "knowledge",
            },
            "window_size_l": 0,
            "window_size_r": 0,
        }
        result = resolve_window(window)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# resolve_context — integration
# Depends on both resolve_window and resolve_loads, both xfail.
# Also: function has no return statement.
# ---------------------------------------------------------------------------

class TestResolveContext:
    @pytest.mark.xfail(
        reason="Depends on resolve_window (Identifier bug) + resolve_loads (addr bug) + no return statement"
    )
    def test_full_integration(self, seed):
        from ..src.python.sceduler.goal_stack.context import resolve_context
        result = resolve_context(seed["slave_obj"])
        assert result is not None

    def test_missing_master_context_row_raises(self, db):
        """
        master_addr with no row in master_context → fetchone() is None →
        window_data[0] blows up with TypeError. Should raise, not silently return.
        """
        m_addr = insert_master(db)
        r_addr = insert_result(db, "x", False)
        s_addr = insert_slave(db, m_addr, f"OrphanSlave{m_addr}",
                               "noop", r_addr, "noop_result")
        slave_obj = dict(addr=s_addr, instruction="noop",
                         master_addr=m_addr, result_name="noop_result")
        from ..src.python.sceduler.goal_stack.context import resolve_context
        with pytest.raises(Exception):
            resolve_context(slave_obj)


# ---------------------------------------------------------------------------
# new_result() DB function — tested directly via SQL
# This is a pure DB-level integration test, no Python code involved.
# ---------------------------------------------------------------------------

class TestNewResultFunction:
    def test_marks_result_ready(self, db, seed):
        """new_result() should flip ready=TRUE and set content."""
        db.execute(
            "SELECT new_result($1, $2, NULL)",
            ("computed output", seed["r_addr"])
        )
        row = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = $1",
            (seed["r_addr"],)
        ).fetchone()
        assert row[0] == "computed output"
        assert row[1] is True

    def test_resolve_by_name(self, db, seed):
        """new_result() p_name path: resolve via slaves.result_name."""
        # Create a fresh result (not ready) with a known result_name
        r2 = insert_result(db, "", False)
        m2 = insert_master(db)
        insert_slave(db, m2, f"NamedSlave{r2}", "x", r2, "named_result_key")
        db.execute(
            "SELECT new_result($1, NULL, $2)",
            ("output via name", "named_result_key")
        )
        row = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = $1", (r2,)
        ).fetchone()
        assert row[0] == "output via name"
        assert row[1] is True

    def test_notifies_unblocked_slave(self, db, seed):
        """
        When a result is the last blocker for a slave, new_result() should
        pg_notify('slaves_ready', slave_addr). Verify by checking slave_req
        state — all reqs must be ready after the call.
        """
        # Build: slave blocked on two results, satisfy both.
        m = insert_master(db)
        r1 = insert_result(db, "", False)
        r2 = insert_result(db, "", False)
        s = insert_slave(db, m, f"BlockedSlave{m}", "x", r1, "r1_name",
                         requires=[r1, r2])

        db.execute("SELECT new_result($1, $2, NULL)", ("first", r1))
        db.execute("SELECT new_result($1, $2, NULL)", ("second", r2))

        # After both are ready, no unsatisfied reqs remain for this slave.
        unsatisfied = db.execute("""
            SELECT 1 FROM slave_req sr
            JOIN results r ON r.addr = sr.req_addr
            WHERE sr.slave_addr = $1 AND r.ready = FALSE
        """, (s,)).fetchone()
        assert unsatisfied is None

    def test_no_addr_no_name_raises(self, db):
        with pytest.raises(psycopg.errors.RaiseException):
            db.execute("SELECT new_result($1, NULL, NULL)", ("x",))


# ---------------------------------------------------------------------------
# new_slave() DB function
# ---------------------------------------------------------------------------

class TestNewSlaveFunction:
    def test_creates_slave_and_name(self, db):
        m = insert_master(db)
        r = insert_result(db, "", False)
        s_addr = db.execute(
            "SELECT new_slave($1,$2,$3,$4,$5,$6)",
            (m, "MyNewSlave", "do stuff", [], r, "my_result")
        ).fetchone()[0]

        slave = db.execute(
            "SELECT instruction, result_name FROM slaves WHERE addr = $1", (s_addr,)
        ).fetchone()
        assert slave[0] == "do stuff"
        assert slave[1] == "my_result"

        name = db.execute(
            "SELECT name FROM names WHERE addr = $1", (s_addr,)
        ).fetchone()
        assert name[0] == "MyNewSlave"

    def test_requires_inserts_slave_req_rows(self, db):
        m = insert_master(db)
        r1 = insert_result(db, "", False)
        r2 = insert_result(db, "", False)
        result_r = insert_result(db, "", False)
        s_addr = db.execute(
            "SELECT new_slave($1,$2,$3,$4,$5,$6)",
            (m, f"ReqSlave{m}", "needs two", [r1, r2], result_r, "req_result")
        ).fetchone()[0]

        reqs = db.execute(
            "SELECT req_addr FROM slave_req WHERE slave_addr = $1 ORDER BY req_addr",
            (s_addr,)
        ).fetchall()
        req_addrs = {row[0] for row in reqs}
        assert r1 in req_addrs
        assert r2 in req_addrs


# ---------------------------------------------------------------------------
# addrs_tables view
# ---------------------------------------------------------------------------

class TestAddrsTablesView:
    def test_knowledge_type(self, db, seed):
        row = db.execute(
            'SELECT type FROM addrs_tables WHERE addr = $1', (seed["k_addr"],)
        ).fetchone()
        assert row is not None
        assert row[0] == "knowledge"

    def test_executable_type(self, db, seed):
        row = db.execute(
            'SELECT type FROM addrs_tables WHERE addr = $1', (seed["e_addr"],)
        ).fetchone()
        assert row[0] == "executables"

    def test_slave_type(self, db, seed):
        row = db.execute(
            'SELECT type FROM addrs_tables WHERE addr = $1', (seed["s_addr"],)
        ).fetchone()
        assert row[0] == "slaves"

    def test_result_type(self, db, seed):
        row = db.execute(
            'SELECT type FROM addrs_tables WHERE addr = $1', (seed["r_addr"],)
        ).fetchone()
        assert row[0] == "results"

    def test_orphan_addr_not_in_view(self, db):
        """An addr that exists in addrs but no content table → not in view."""
        orphan = new_addr(db)
        row = db.execute(
            'SELECT type FROM addrs_tables WHERE addr = $1', (orphan,)
        ).fetchone()
        assert row is None

