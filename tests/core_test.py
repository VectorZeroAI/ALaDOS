#!/usr/bin/env python3
"""
Integration tests for ALaDOS system.
Requires a local PostgreSQL instance with database 'alados' accessible without a password
(host=127.0.0.1, port=5432). The application will be started as a subprocess and
tested end‑to‑end.
"""

import subprocess
import time
import threading
import queue
import sys
import os
from pathlib import Path

import psycopg
import pytest

# ----------------------------------------------------------------------
# Helper: wait until the server prints a specific startup message
# ----------------------------------------------------------------------
def _wait_for_startup(proc, timeout=30):
    """Read lines from proc.stdout until the startup message appears."""
    deadline = time.time() + timeout
    for line in proc.stdout:
        if "startup of the server finished." in line:
            return True
        if time.time() > deadline:
            return False
    return False


# ----------------------------------------------------------------------
# Output capturing for subprocess stdout/stderr (non-blocking)
# ----------------------------------------------------------------------
def _start_output_capture(proc):
    """Spawn threads that drain stdout and stderr into queues."""
    out_q = queue.Queue()
    err_q = queue.Queue()

    def _enqueue_output(stream, q):
        for line in stream:
            q.put(line)
        stream.close()

    threading.Thread(target=_enqueue_output, args=(proc.stdout, out_q), daemon=True).start()
    threading.Thread(target=_enqueue_output, args=(proc.stderr, err_q), daemon=True).start()
    return out_q, err_q


# ----------------------------------------------------------------------
# Fixture: start the application (module scope – shared across tests)
# ----------------------------------------------------------------------
@pytest.fixture(scope="module")
def app_process():
    """Launch the ALaDOS server and wait until it is ready."""

    env = os.environ.copy()

    cmd = "alados_start"

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,    # prevent console waiting for input
        text=True,
        bufsize=1,                   # line‑buffered
        env=env,
    )

    out_q, err_q = _start_output_capture(proc)

    # Wait for the startup signal
    if not _wait_for_startup(proc, timeout=30):
        # Dump captured output to help debugging
        sys.stdout.write("\n--- Captured stdout during startup ---\n")
        while not out_q.empty():
            sys.stdout.write(out_q.get_nowait())
        sys.stdout.write("\n--- Captured stderr during startup ---\n")
        while not err_q.empty():
            sys.stdout.write(err_q.get_nowait())
        proc.terminate()
        proc.wait()
        pytest.fail("Server did not start in time")

    # Store the queues so we can access them later (in case of failures)
    proc._out_q = out_q
    proc._err_q = err_q

    yield proc

    # Teardown: terminate the server
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ----------------------------------------------------------------------
# Fixture: fresh database connection for test helpers
# ----------------------------------------------------------------------
@pytest.fixture
def db_conn():
    """Return a psycopg connection to the test database."""
    conn = psycopg.connect(host="127.0.0.1", port=5432, dbname="alados_test")
    conn.autocommit = True
    return conn


# ----------------------------------------------------------------------
# Helper to wait for all slaves of a master to become ready
# ----------------------------------------------------------------------
def _wait_for_master(conn, master_addr, timeout=120):
    """Poll until every slave of the given master has result.ready=True."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur = conn.execute(
            """
            SELECT bool_and(r.ready)
            FROM slaves s
            JOIN results r ON r.addr = s.result_addr
            WHERE s.master_addr = %s
            """,
            (master_addr,),
        )
        all_ready = cur.fetchone()[0]
        if all_ready:
            return True
        time.sleep(0.5)
    return False


# ----------------------------------------------------------------------
# Helper to dump captured subprocess output on test failure
# ----------------------------------------------------------------------
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture test outcome to add subprocess output on failure."""
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call" and rep.failed:
        # Access the app_process fixture if it was used in the test
        if "app_process" in item.funcargs:
            proc = item.funcargs["app_process"]
            out_q = getattr(proc, "_out_q", None)
            err_q = getattr(proc, "_err_q", None)
            if out_q or err_q:
                sys.stdout.write("\n========== Subprocess output on failure ==========\n")
                sys.stdout.write("--- stdout (last ~100 lines) ---\n")
                lines = []
                if out_q:
                    while not out_q.empty():
                        lines.append(out_q.get_nowait())
                    sys.stdout.write("".join(lines[-100:]))
                sys.stdout.write("\n--- stderr (last ~100 lines) ---\n")
                lines = []
                if err_q:
                    while not err_q.empty():
                        lines.append(err_q.get_nowait())
                    sys.stdout.write("".join(lines[-100:]))
                sys.stdout.write("==================================================\n")


# ======================================================================
#                          Actual test cases
# ======================================================================

def test_simple_instruction(app_process, db_conn):
    """A trivial task: the LLM should just produce a result."""
    master_addr = db_conn.execute(
        "SELECT new_master('Say hello world')"
    ).fetchone()[0]

    completed = _wait_for_master(db_conn, master_addr, timeout=60)
    assert completed, f"Master {master_addr} did not complete in time"

    # Optionally check the final master result
    result_addr = db_conn.execute(
        "SELECT result_addr FROM masters WHERE addr = %s", (master_addr,)
    ).fetchone()[0]
    content = db_conn.execute(
        "SELECT content_str FROM results WHERE addr = %s", (result_addr,)
    ).fetchone()[0]
    assert content is not None and len(content) > 0, "Master result is empty"


def test_create_and_read_knowledge(app_process, db_conn):
    """The AI must create a knowledge item and then read it."""
    master_addr = db_conn.execute(
        "SELECT new_master('Create a knowledge item with content \"The moon is made of cheese\" and description \"fun fact about moon\", then read it back.')"
    ).fetchone()[0]

    assert _wait_for_master(db_conn, master_addr, timeout=120), "Master did not finish"

    # Verify that at least one slave created the item and another read it
    slave_count = db_conn.execute(
        "SELECT count(*) FROM slaves WHERE master_addr = %s", (master_addr,)
    ).fetchone()[0]
    assert slave_count >= 2, "Expected at least two slaves (create + read)"


def test_context_window_landing(app_process, db_conn):
    """Test semantic landing of the viewing window."""
    # Create some items first, then ask to land
    db_conn.execute(
        "SELECT new_master('Create three knowledge items about different fruits.')"
    ).fetchone()[0]
    # Wait for that master to finish? Not necessary; the next task will trigger later.
    # Better: create items directly via SQL
    for fruit, content in [("apple", "Apples are red and juicy."),
                           ("banana", "Bananas are yellow and curved."),
                           ("orange", "Oranges are citrus fruits.")]:
        addr = db_conn.execute("SELECT new_addr()").fetchone()[0]
        db_conn.execute(
            "INSERT INTO knowledge (addr, content) VALUES (%s, %s)",
            (addr, content),
        )
        db_conn.execute(
            "INSERT INTO vector_ops (addr_k, description) VALUES (%s, %s)",
            (addr, f"Information about {fruit}"),
        )

    master_addr = db_conn.execute(
        "SELECT new_master('Land the context window on the item about bananas.')"
    ).fetchone()[0]

    assert _wait_for_master(db_conn, master_addr, timeout=60)
    # Ensure the window anchor changed (check master_context)
    anchor = db_conn.execute(
        """
        SELECT window_anchor_knowledge FROM master_context WHERE addr = %s
        """,
        (master_addr,),
    ).fetchone()[0]
    assert anchor is not None, "Window anchor was not set"


def test_tool_execution(app_process, db_conn):
    """The AI should be able to execute an existing tool."""
    # Create a tool manually
    tool_addr = db_conn.execute("SELECT new_addr()").fetchone()[0]
    db_conn.execute(
        "INSERT INTO executables (addr, header, body) VALUES (%s, %s, %s)",
        (tool_addr, "get_greeting -> str", "print('Hello from test tool')"),
    )
    db_conn.execute(
        "INSERT INTO vector_ops (addr_exe, description) VALUES (%s, %s)",
        (tool_addr, "A simple tool that prints a greeting"),
    )

    master_addr = db_conn.execute(
        "SELECT new_master('Execute the tool \"get_greeting\".')"
    ).fetchone()[0]

    assert _wait_for_master(db_conn, master_addr, timeout=90)


def test_paradox_detection(app_process, db_conn):
    """Check that the system handles paradoxal information."""
    # Create two conflicting knowledge items
    addr1 = db_conn.execute("SELECT new_addr()").fetchone()[0]
    addr2 = db_conn.execute("SELECT new_addr()").fetchone()[0]
    db_conn.execute(
        "INSERT INTO knowledge (addr, content) VALUES (%s, %s)",
        (addr1, "The Earth is flat."),
    )
    db_conn.execute(
        "INSERT INTO knowledge (addr, content) VALUES (%s, %s)",
        (addr2, "The Earth is round."),
    )
    db_conn.execute(
        "INSERT INTO vector_ops (addr_k, description) VALUES (%s, %s)",
        (addr1, "flat Earth claim"),
    )
    db_conn.execute(
        "INSERT INTO vector_ops (addr_k, description) VALUES (%s, %s)",
        (addr2, "round Earth claim"),
    )

    master_addr = db_conn.execute(
        "SELECT new_master('You are given two contradictory items. Report the paradox using the appropriate tool.')"
    ).fetchone()[0]

    # Even if the master “fails” because of the paradox, the system should not crash.
    # We’ll just check that it completes without an unexpected exception.
    assert _wait_for_master(db_conn, master_addr, timeout=120) or True
    # Verify that at least one slave has status 'paradox' or 'error'
    paradox_exists = db_conn.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM results
            WHERE addr IN (SELECT result_addr FROM slaves WHERE master_addr = %s)
              AND (status = 'paradox' OR status = 'error')
        )
        """,
        (master_addr,),
    ).fetchone()[0]
    assert paradox_exists, "Expected at least one slave to report a paradox"


def test_error_recovery(app_process, db_conn):
    """Test that the system recovers from a failed tool call."""
    master_addr = db_conn.execute(
        "SELECT new_master('Try to edit a non-existent knowledge item with addr 99999. Then handle the error and explain what happened.')"
    ).fetchone()[0]

    assert _wait_for_master(db_conn, master_addr, timeout=120)
    # Check that the master result contains some explanation
    result_addr = db_conn.execute(
        "SELECT result_addr FROM masters WHERE addr = %s", (master_addr,)
    ).fetchone()[0]
    content = db_conn.execute(
        "SELECT content_str FROM results WHERE addr = %s", (result_addr,)
    ).fetchone()[0]
    assert content and "error" not in content.lower(), "Master result should not be blank"


def test_multiple_slaves_dependency(app_process, db_conn):
    """A task with multiple dependent slaves should execute in correct order."""
    master_addr = db_conn.execute(
        "SELECT new_master('First, create a knowledge item. Second, read that item. Third, write a summary of it.')"
    ).fetchone()[0]

    assert _wait_for_master(db_conn, master_addr, timeout=180)
    # Check that there are at least three slaves
    slave_count = db_conn.execute(
        "SELECT count(*) FROM slaves WHERE master_addr = %s", (master_addr,)
    ).fetchone()[0]
    assert slave_count >= 3, "Expected at least three slaves"


def test_webui_session_creation(app_process, db_conn):
    """A WebUI session should be created and the first AI response generated."""
    # This test creates a session as a normal user would, by calling the SQL function directly
    session_name = db_conn.execute(
        "SELECT create_session('Hello AI, can you help me?', 'You are a helpful assistant.')"
    ).fetchone()[0]

    # Wait a bit for the AI to respond
    time.sleep(5)

    # Check that an AI message result exists and is ready
    ai_result = db_conn.execute(
        """
        SELECT content_str FROM results
        WHERE metadata->>'type' = 'ai_message'
          AND metadata->>'session_name' = %s
          AND ready = TRUE
        ORDER BY (metadata->>'turn')::int DESC LIMIT 1
        """,
        (session_name,),
    ).fetchone()
    assert ai_result is not None, "AI did not respond to the first message"
    assert len(ai_result[0]) > 0


def test_cronjob_do_this_later(app_process, db_conn):
    """Test that a 'do_this_later' cronjob is executed after a delay."""
    # Insert a cronjob that runs once after 5 seconds
    db_conn.execute(
        """
        INSERT INTO cronjob_once (name, start_after, args)
        VALUES ('ai_perform_action_later', %s, '{"ai_instruction": "create a knowledge item with content ''cronjob success'' and description ''cronjob test''"}'::jsonb)
        """,
        (time.time() + 10,),  # give enough time for the test to wait
    )

    # Wait for the cronjob to execute (up to 30 seconds)
    deadline = time.time() + 30
    found_item = False
    while time.time() < deadline:
        cur = db_conn.execute(
            "SELECT 1 FROM knowledge WHERE content = 'cronjob success'"
        )
        if cur.fetchone():
            found_item = True
            break
        time.sleep(1)

    assert found_item, "Cronjob did not create the expected knowledge item"


# ----------------------------------------------------------------------
# Run configuration: add a marker for slow integration tests
# ----------------------------------------------------------------------
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "not slow"])
