#!/usr/bin/env python3
"""
My core testing framework.
Uses a decorator to turn classes with steps: list[ExecutorStep] into actual tests
and runs them against actual executor thread. 

Usage doc:
    ExecutorStep fields are self explanatory.
    Multiple LLM outputs are allowed when required, as that property is a list of strings.
    Each Step is its own Slave to be executed.
    Each class is its own test case to be executed. 
    You are not required to name classes with Test...
    DB is cleared between classes, e.g. test cases, not between individual Steps. 

!!! RUN THE SQL AT THE BOTTOM AGAINST THE alados_test DB YOURSELF ELSE EVERYTHING TIMES OUT. !!!
"""

import threading
import queue
import time
import dataclasses
from typing import List, Optional

import flask
import httpx
import psycopg
import pytest
import uuid

#  --  System modules  -------------------------------------------------
from python.executor.main import core as executor_core
from python.executor.queue import executor_queue
from python.executor.types import Api
from python.sceduler.main import setup as scheduler_setup
from python.utils.conn_factory import (
    Conn,
    register_all_the_composite_types
)
from python.base_state.main import startup as bs_startup
from python.events.main import startup as ev_startup

#  --  Database configuration  ----------------------------------------
DB_HOST = "/data/data/com.termux/files/usr/tmp"
DB_NAME = "alados_test"


def _test_conn_factory_raw(db_name: str | None = None):
    conn = Conn.connect(host=DB_HOST, dbname=db_name or DB_NAME)
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
            for notify in listen_conn.notifies(timeout=9):
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

    Replaces ${{slave_addr}} with slave addr and ${{result_addr}} with result_addr.
    """
    # We'll create a new class that inherits from object so pytest discovers it.
    class TestWrapper:
        @pytest.fixture(autouse=True)
        def _setup(self, global_setup, clean_database):
            self.mock = global_setup          # the LLMMockServer instance
            self.db = _test_conn_factory()    # fresh connection for the test

        @pytest.mark.parametrize("step", cls.steps, ids=lambda s: s.instruction)
        def test_step(self, step):

            # Create a slave with no dependencies (easy to test)
            slave_addr = self.db.execute_fetchval(
                "SELECT new_slave(NULL, %s, NULL, NULL, NULL, NULL, NULL, 'general')",
                (step.instruction,),
            )
            result_addr = self.db.execute_fetchval(
                "SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,)
            )


            # Replace keys with addresses.
            # ${{slave_addr}} for slave_addr
            # ${{result_addr}} for result_addr
            for i in range(len(step.llm_responses)):
                step.llm_responses[i] = step.llm_responses[i].replace("${{slave_addr}}", str(slave_addr))
                step.llm_responses[i] = step.llm_responses[i].replace("${{result_addr}}", str(result_addr))

            # Enqueue responses for this step
            for resp in step.llm_responses:
                self.mock.put_response(resp)

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

    coros = bs_startup()
    # Core tests need base-state registrations, but starting the event system
    # here creates persistent event consumers that pollute event_tests.py.
    for coro in coros:
        close = getattr(coro, "close", None)
        if close is not None:
            close()

    yield mock_llm


@pytest.fixture(autouse=True, scope="class")
def clean_database():
    """Clean tables before each test function."""
    conn = _test_conn_factory()
    _clean_database(conn)
    conn.close()


def _clean_database(conn: Conn):
    tables = [
        # Children/dependency rows first.
        "event_call_fill_result", "event_call_execute_slave", "event_call_rmt",
        "event_consumers",
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves",
        "cronjob_once", "cronjob_loop",
        # Items referenced by the rows above.
        "reusable_master_templates",
        "vector_ops",
        "executables", "knowledge",
        "names",
        "slaves", "masters",
        "results",
        "addrs",
    ]
    with conn.transaction():
        for table in tables:
            conn.execute(f"DELETE FROM {table}")  # pyright: ignore
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")
