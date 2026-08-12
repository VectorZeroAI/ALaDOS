#!/usr/bin/env python3
"""
Conn factory test.
"""
import pytest
from python.utils.conn_factory import conn_factory, conn_factory_raw, NoValue

@pytest.fixture
def conn():
    db = conn_factory("alados_test")
    db.autocommit = False
    db.execute("BEGIN")
    yield db
    db.execute("ROLLBACK")
    db.close()

def test_execute_fetchval_returns_value(conn):
    conn.execute("CREATE TEMP TABLE test_fetch (id serial, val text) ON COMMIT DROP")
    conn.execute("INSERT INTO test_fetch (val) VALUES ('hello')")
    result = conn.execute_fetchval("SELECT val FROM test_fetch WHERE id = 1")
    assert result == "hello"

def test_execute_fetchval_raises_runtime_error_on_empty(conn):
    with pytest.raises(RuntimeError):
        conn.execute_fetchval("SELECT 1 WHERE FALSE")

def test_executemany_returning(conn):
    conn.execute("CREATE TEMP TABLE test_em (id serial, val text) ON COMMIT DROP")
    conn.executemany(
        "INSERT INTO test_em (val) VALUES (%s) RETURNING id, val",
        [("a",), ("b",)],
        returning=True
    )
    rows = conn.execute("SELECT id, val FROM test_em ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "a"
    assert rows[1][1] == "b"

def test_executemany_returning_with_fetch(conn):
    conn.execute("CREATE TEMP TABLE test_em2 (id serial, num int) ON COMMIT DROP")
    rows = conn.executemany(
        "INSERT INTO test_em2 (num) VALUES (%s) RETURNING id, num",
        [(1,), (2,)],
        returning=True
    )
    results = []
    for row in rows:
        results.append(row[1])   # num column
    assert len(results) == 2
    assert results[0] == 1
    assert results[1] == 2

def test_executemany_without_returning(conn):
    conn.execute("CREATE TEMP TABLE test_em3 (val text) ON COMMIT DROP")
    result = conn.executemany(
        "INSERT INTO test_em3 (val) VALUES (%s)",
        [("x",), ("y",)],
        returning=False
    )
    assert result is None

def test_conn_factory_accepts_db_name():
    conn = conn_factory("alados_test")
    db_name = conn.execute_fetchval("SELECT current_database()")
    assert db_name == "alados_test"
    conn.close()

def test_conn_factory_raw_with_db_name():
    conn = conn_factory_raw("alados_test")
    db_name = conn.execute_fetchval("SELECT current_database()")
    assert db_name == "alados_test"
    conn.close()


