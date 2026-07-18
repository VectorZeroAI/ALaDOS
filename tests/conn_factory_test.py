#!/usr/bin/env python3

"""
Conn factory test. 
"""

import pytest
from ALaDOS.src.python.utils.conn_factory import conn_factory, NoValue

@pytest.fixture
def conn():
    """Fresh connection within a rollback transaction."""
    db = conn_factory()
    db.autocommit = False
    db.execute("BEGIN")
    yield db
    db.execute("ROLLBACK")
    db.close()

def test_execute_fetchval_returns_value(conn):
    """execute_fetchval should return the first column of the first row."""
    conn.execute("CREATE TEMP TABLE test_fetch (id serial, val text) ON COMMIT DROP")
    conn.execute("INSERT INTO test_fetch (val) VALUES ('hello')")
    result = conn.execute_fetchval("SELECT val FROM test_fetch WHERE id = 1")
    assert result == "hello"

def test_execute_fetchval_raises_runtime_error_on_empty(conn):
    """Should raise RuntimeError when query returns no rows."""
    with pytest.raises(RuntimeError):
        conn.execute_fetchval("SELECT 1 WHERE FALSE")

def test_executemany_returning(conn):
    """executemany(..., returning=True) should yield result rows."""
    conn.execute("CREATE TEMP TABLE test_em (id serial, val text) ON COMMIT DROP")
    # Insert two rows
    conn.executemany(
        "INSERT INTO test_em (val) VALUES (%s) RETURNING id, val",
        [("a",), ("b",)],
        returning=True
    )
    # The above should have inserted two rows; we check by selecting.
    rows = conn.execute("SELECT id, val FROM test_em ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "a"
    assert rows[1][1] == "b"

def test_executemany_returning_with_fetch(conn):
    """Test that the returned cursors actually contain data."""
    conn.execute("CREATE TEMP TABLE test_em2 (id serial, num int) ON COMMIT DROP")
    # Use the same approach as resolve_to_addrs: call executemany and iterate.
    rows = conn.executemany(
        "INSERT INTO test_em2 (num) VALUES (%s) RETURNING id, num",
        [(1,), (2,)],
        returning=True
    )

    results = []
    for row in rows:
        results.append(row[0])

    assert len(results) == 2
    assert results[0] == 1
    assert results[1] == 2

def test_executemany_without_returning(conn):
    """executemany(..., returning=False) should return None."""
    conn.execute("CREATE TEMP TABLE test_em3 (val text) ON COMMIT DROP")
    result = conn.executemany(
        "INSERT INTO test_em3 (val) VALUES (%s)",
        [("x",), ("y",)],
        returning=False
    )
    assert result is None
