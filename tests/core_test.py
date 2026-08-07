#!/usr/bin/env python3
"""
Declarative integration tests for the ALaDOS executor core.
Uses a real database, a real executor thread, a real scheduler,
and a Flask mock for the LLM API.

Tests are defined as simple class‑based scenarios with a list of steps.
Each step specifies what the LLM should return and what assertions to
perform on the completed slave's result.

The database sends NOTIFY when a result becomes ready,
so we wait with LISTEN instead of polling.

Run once against your test DB:
    CREATE OR REPLACE FUNCTION notify_result_ready()
    RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.ready AND NOT OLD.ready THEN
            PERFORM pg_notify('result_ready', NEW.addr::TEXT);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER trg_result_ready
    AFTER UPDATE ON results
    FOR EACH ROW EXECUTE FUNCTION notify_result_ready();
"""

import threading
import queue
import time
import dataclasses
from typing import List, Optional, Callable

import flask
import httpx
import psycopg
import pytest

#  --  System modules  -------------------------------------------------
from python.executor.main import core as executor_core
from python.executor.queue import executor_queue
from python.executor.types import Api
from python.sceduler.main import setup as scheduler_setup
from python.utils.conn_factory import (
    Conn,
    register_all_the_composite_types
)

#  --  Database configuration  ----------------------------------------
DB_HOST = "/data/data/com.termux/files/usr/tmp"
DB_NAME = "alados_test"


def _test_conn_factory_raw():
    conn = Conn.connect(host=DB_HOST, dbname=DB_NAME)
    conn.autocommit = True
    return conn


def _test_conn_factory():
    raw = _test_conn_factory_raw()
    conn = register_all_the_composite_types(raw)
    return conn


#  --  Monkey‑patch all modules that use conn_factory  ----------------
def _patch_connection_factories():
    import python.executor.main as executor_main
    import python.sceduler.main as scheduler_main
    import python.utils.logger as logger_mod
    executor_main.conn_factory = _test_conn_factory
    scheduler_main.conn_factory = _test_conn_factory
    logger_mod.conn_factory = _test_conn_factory
    import python.utils.conn_factory as conn_mod
    conn_mod.conn_factory_raw = _test_conn_factory_raw


#  --  Flask LLM mock  ------------------------------------------------
class LLMMockServer:
    def __init__(self):
        self.app = flask.Flask(__name__)
        self.response_queue = queue.Queue()
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/v1/chat/completions", methods=["POST"])
        def chat_completions():
            try:
                content = self.response_queue.get(timeout=30)
            except queue.Empty:
                return flask.jsonify({"error": "No mock response left"}), 500
            return flask.jsonify({"choices": [{"message": {"content": content}}]})

    def start(self, port: int = 8001) -> int:
        self.thread = threading.Thread(
            target=self.app.run,
            kwargs={"port": port, "debug": False, "use_reloader": False},
            daemon=True,
        )
        self.thread.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=0.2)
                break
            except httpx.RequestError:
                time.sleep(0.1)
        self.port = port
        return self.port

    def put_response(self, json_string: str):
        self.response_queue.put(json_string)


#  --  Helper: wait for result using LISTEN/NOTIFY  -------------------
def wait_for_result(db_conn: Conn, result_addr: int, timeout: float = 10.0):
    """
    Block until the given result is marked ready.
    Uses the 'result_ready' notification channel.
    """
    # We need a separate connection for listening because
    # psycopg can't listen on a connection that's also doing other work.
    with psycopg.connect(host=DB_HOST, dbname=DB_NAME) as listen_conn:
        listen_conn.autocommit = True
        listen_conn.execute("LISTEN result_ready")
        deadline = time.time() + timeout
        while time.time() < deadline:
            for notify in listen_conn.notifies(timeout=0.5):
                if notify.payload == str(result_addr):
                    return
        raise TimeoutError(f"Result {result_addr} not ready within {timeout}s")


#  --  Declarative test step  -----------------------------------------
@dataclasses.dataclass
class ExecutorStep:
    """One step of an executor test scenario."""
    instruction: str
    llm_responses: List[str]                    # will be queued in order
    expected_content_contains: Optional[str] = None
    expected_status: Optional[str] = None
    expected_knowledge_count: Optional[int] = None  # example of a DB assertion


#  --  Decorator that turns a step class into a pytest test class  ----
def executor_test(cls):
    """
    Decorator that reads the `steps` class variable and creates a
    parametrized pytest test class. Each step is run in order.
    """
    # We'll create a new class that inherits from object so pytest discovers it.
    class TestWrapper:
        @pytest.fixture(autouse=True)
        def _setup(self, global_setup, clean_database):
            self.mock = global_setup          # the LLMMockServer instance
            self.db = _test_conn_factory()    # fresh connection for the test

        @pytest.mark.parametrize("step", cls.steps, ids=lambda s: s.instruction)
        def test_step(self, step):
            # Enqueue responses for this step
            for resp in step.llm_responses:
                self.mock.put_response(resp)

            # Create a slave with no dependencies (easy to test)
            slave_addr = self.db.execute_fetchval(
                "SELECT new_slave(NULL, %s, NULL, NULL, NULL, NULL, NULL, 'general')",
                (step.instruction,),
            )
            result_addr = self.db.execute_fetchval(
                "SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,)
            )

            # Wait for the executor to process it
            wait_for_result(self.db, result_addr)

            # Run the user‑specified assertions
            if step.expected_content_contains is not None:
                content = self.db.execute_fetchval(
                    "SELECT content_str FROM results WHERE addr = %s", (result_addr,)
                )
                assert step.expected_content_contains in content, (
                    f"Expected '{step.expected_content_contains}' in result, "
                    f"got: {content}"
                )

            if step.expected_status is not None:
                status = self.db.execute_fetchval(
                    "SELECT status FROM results WHERE addr = %s", (result_addr,)
                )
                assert status == step.expected_status, (
                    f"Expected status '{step.expected_status}', got '{status}'"
                )

            if step.expected_knowledge_count is not None:
                cnt = self.db.execute_fetchval("SELECT count(*) FROM knowledge")
                assert cnt == step.expected_knowledge_count, (
                    f"Expected {step.expected_knowledge_count} knowledge items, "
                    f"found {cnt}"
                )

    # Give the wrapper a nice name for pytest output
    TestWrapper.__name__ = "Test" + cls.__name__
    TestWrapper.__qualname__ = "Test" + cls.__qualname__
    
    globals()[TestWrapper.__name__] = TestWrapper
    globals()[TestWrapper.__qualname__] = TestWrapper

    return TestWrapper


#  --  Session‑scoped fixture for the executor and mock server  -------
@pytest.fixture(scope="session", autouse=True)
def global_setup():
    """Patch factories, start scheduler and executor with mock API."""
    _patch_connection_factories()

    mock_llm = LLMMockServer()
    port = mock_llm.start()
    api = Api(
        url=f"http://127.0.0.1:{port}/v1/chat/completions",
        key="test",
        model="mock",
        max_tokens=8000,
    )

    scheduler_setup()
    threading.Thread(
        target=executor_core, args=(executor_queue, [api]), daemon=True
    ).start()

    # Clean database once at the start of the session
    conn = _test_conn_factory()
    _clean_database(conn)
    conn.close()

    yield mock_llm


@pytest.fixture(autouse=True)
def clean_database():
    """Clean tables before each test function."""
    conn = _test_conn_factory()
    _clean_database(conn)
    conn.close()


def _clean_database(conn: Conn):
    tables = [
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves", "reusable_master_templates",
        "slaves", "masters", "results", "names", "vector_ops",
        "executables", "knowledge", "logs", "addrs",
        "cronjob_once", "cronjob_loop",
        "event_consumers", "event_call_rmt", "event_call_execute_slave",
        "event_call_fill_result",
    ]
    with conn.transaction():
        for t in tables:
            try:
                conn.execute(f"DELETE FROM {t} CASCADE")  # pyright: ignore
            except Exception:
                pass
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")


#  ======================================================================
#  Example test cases – just add classes like these
#  ======================================================================

@executor_test
class BasicExecution:
    steps = [
        ExecutorStep(
            instruction="Write 'Hello World'",
            llm_responses=['[{"tool": "result.write", "args": {"text": "Hello World"}}]'],
            expected_content_contains="Hello World",
        ),
    ]


@executor_test
class ParadoxHandling:
    steps = [
        ExecutorStep(
            instruction="Report a paradox",
            llm_responses=[
                '[{"tool": "K.report_paradoxal_information", "args": {"items": [1], "paradox": "test"}}]'
            ],
            expected_status="paradox",
        ),
    ]


@executor_test
class ErrorRecovery:
    steps = [
        ExecutorStep(
            instruction="Fail then recover",
            llm_responses=[
                '[{"tool": "nonexistent.tool", "args": {}}]',
                '[{"tool": "result.write", "args": {"text": "recovered"}}]',
            ],
            expected_content_contains="recovered",
        ),
    ]


@executor_test
class CreateAndReadKnowledge:
    steps = [
        ExecutorStep(
            instruction="Create a knowledge item",
            llm_responses=[
                '[{"tool": "K.create", "args": {"content": "moon is cheese", "description": "fun fact"}}]'
            ],
            expected_knowledge_count=1,
        ),
        ExecutorStep(
            instruction="Read that knowledge item (simulate read by checking we can find it)",
            llm_responses=[
                '[{"tool": "K.read", "args": {"id": 1}}]'  # first knowledge item is addr 1
            ],
            expected_content_contains="moon is cheese",
        ),
    ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
CREATE OR REPLACE FUNCTION notify_result_ready()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.ready AND NOT OLD.ready THEN
        PERFORM pg_notify('result_ready', NEW.addr::TEXT);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_result_ready
AFTER UPDATE ON results
FOR EACH ROW EXECUTE FUNCTION notify_result_ready();
"""
