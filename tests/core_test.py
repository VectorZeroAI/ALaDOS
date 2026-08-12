#!/usr/bin/env python3
"""
My core testing framework.
"""
import dataclasses
import json
import os
import queue
import threading
import time
import uuid
from typing import List, Optional

import flask
import httpx
import psycopg
import pytest

from python.executor.main import core as executor_core
from python.executor.queue import executor_queue
from python.executor.types import Api
from python.sceduler.main import setup as scheduler_setup
from python.utils.conn_factory import conn_factory
from python.base_state.main import startup as bs_startup
from python.events.main import startup as ev_startup


os.environ.setdefault("ALADOS_DB_NAME", "alados_test")  # ensure test DB


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


def wait_for_result(db_conn, result_addr: int, timeout: float = 10.0):
    with psycopg.connect(host="/data/data/com.termux/files/usr/tmp", dbname="alados_test") as listen_conn:
        listen_conn.autocommit = True
        listen_conn.execute("LISTEN result_ready")
        deadline = time.time() + timeout
        while time.time() < deadline:
            for notify in listen_conn.notifies(timeout=9):
                if notify.payload == str(result_addr):
                    return
        raise TimeoutError(f"Result {result_addr} not ready within {timeout}s")


@dataclasses.dataclass
class ExecutorStep:
    instruction: str
    llm_responses: List[str]
    expected_content_contains: Optional[str] = None
    expected_status: Optional[str] = None
    expected_knowledge_count: Optional[int] = None


def executor_test(cls):
    class TestWrapper:
        @pytest.fixture(autouse=True)
        def _setup(self, global_setup, clean_database):
            self.mock = global_setup
            self.db = conn_factory("alados_test")

        @pytest.mark.parametrize("step", cls.steps, ids=lambda s: s.instruction)
        def test_step(self, step):
            slave_addr = self.db.execute_fetchval(
                "SELECT new_slave(NULL, %s, NULL, NULL, NULL, NULL, NULL, 'general')",
                (step.instruction,),
            )
            result_addr = self.db.execute_fetchval(
                "SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,)
            )
            # substitute placeholders
            for i in range(len(step.llm_responses)):
                step.llm_responses[i] = step.llm_responses[i].replace("${{slave_addr}}", str(slave_addr))
                step.llm_responses[i] = step.llm_responses[i].replace("${{result_addr}}", str(result_addr))
            for resp in step.llm_responses:
                self.mock.put_response(resp)
            wait_for_result(self.db, result_addr)
            if step.expected_content_contains is not None:
                content = self.db.execute_fetchval(
                    "SELECT content_str FROM results WHERE addr = %s", (result_addr,)
                )
                assert step.expected_content_contains in content, (
                    f"Expected '{step.expected_content_contains}' in result, got: {content}"
                )
            if step.expected_status is not None:
                status = self.db.execute_fetchval(
                    "SELECT status FROM results WHERE addr = %s", (result_addr,)
                )
                assert status == step.expected_status
            if step.expected_knowledge_count is not None:
                cnt = self.db.execute_fetchval("SELECT count(*) FROM knowledge")
                assert cnt == step.expected_knowledge_count

    TestWrapper.__name__ = "Test" + cls.__name__
    TestWrapper.__qualname__ = "Test" + cls.__qualname__
    globals()[TestWrapper.__name__] = TestWrapper
    globals()[TestWrapper.__qualname__] = TestWrapper
    return TestWrapper


@pytest.fixture(scope="session", autouse=True)
def global_setup():
    mock_llm = LLMMockServer()
    port = mock_llm.start()
    api = Api(url=f"http://127.0.0.1:{port}/v1/chat/completions", key="test", model="mock", max_tokens=8000)
    scheduler_setup()
    threading.Thread(target=executor_core, args=(executor_queue, [api]), daemon=True).start()
    conn = conn_factory("alados_test")
    _clean_database(conn)
    conn.close()
    coros = bs_startup()
    ev_startup(coros)
    yield mock_llm


@pytest.fixture(autouse=True, scope="class")
def clean_database():
    conn = conn_factory("alados_test")
    _clean_database(conn)
    conn.close()


def _clean_database(conn):
    tables = [
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves", "reusable_master_templates",
        "slaves", "masters", "results", "names", "vector_ops",
        "executables", "knowledge",
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
        # Delete only user‑created addresses (positive ids) to keep base‑state items (negative ids)
        conn.execute("DELETE FROM addrs WHERE addr > 0")
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")


# ----------------- test cases -----------------

@executor_test
class BasicExecution:
    steps = [
        ExecutorStep(
            instruction="Write 'Hello World'",
            llm_responses=['[{"tool": "result_write", "args": {"text": "Hello World"}}]'],
            expected_content_contains="Hello World",
        ),
    ]


@executor_test
class ParadoxHandling:
    steps = [
        ExecutorStep(
            instruction="Report a paradox",
            llm_responses=[
                '[{"tool": "k_report_paradoxal_information", "args": {"items": [1], "paradox": "test"}}]',
                '[{"tool": "result_write", "args": {"text": "assume handled."}}]',
                '[{"tool": "result_write", "args": {"text": "done!"}}]'
            ],
            expected_content_contains="done!"
        ),
    ]


@executor_test
class ErrorRecovery:
    steps = [
        ExecutorStep(
            instruction="Fail then recover",
            llm_responses=[
                '[{"tool": "nonexistent_tool", "args": {}}]',
                '[{"tool": "result_write", "args": {"text": "recovered"}}]',
            ],
            expected_content_contains="recovered",
        ),
    ]

name = str(uuid.uuid4())

@executor_test
class CreateAndReadKnowledge:
    steps = [
        ExecutorStep(
            instruction="Create a knowledge item",
            llm_responses=[
                '[{"tool": "k_create", "args": {"content": "moon is cheese", "description": "fun fact", "name": "' + name + '"}}]'
            ],
            expected_knowledge_count=1,
        ),
        ExecutorStep(
            instruction="Read that knowledge item (simulate read by checking we can find it)",
            llm_responses=[
                '[{"tool": "k_read", "args": {"id": "' + name + '"}}]'
            ],
            expected_content_contains="moon is cheese",
        ),
    ]


@executor_test
class CheckNestedNewPaths:
    steps = [
        ExecutorStep(
            instruction="Start wrongly.",
            llm_responses=[
                '[{"tool": "NOT EXISTS", "args": {}}]',
                '[{"tool": "k_report_paradoxal_information", "args": {"items": [999], "paradox": "test"}}]',
                '[{"tool": "result_write", "args": {"text": "recovered paradox"}}]',
                '[{"tool": "result_write", "args": {"text": "recovered tool_error"}}]',
            ],
            expected_content_contains="recovered tool_error"
        )
    ]

uuid = str(uuid.uuid4())

@executor_test
class CheckToolExecuteBuiltinFunc:
    steps = [
        ExecutorStep(
            instruction="Create a test tool that uses a syscall.",
            llm_responses=[
                '''[
                    {
                        "tool": "tool_create",
                        "args": {
                            "description": "desc",
                            "body": "from ALaDOS.lib.Knowledge import create; import asyncio; asyncio.run(create(${{slave_addr}}, \\"Content\\", \\"description\\")); print(\\"EXECUTED CORRECTLY.\\")",
                            "header": "Nothing.",
                            "name": "'''+ uuid +'"}}]'
            ]
        ),
        ExecutorStep(
            instruction="Use the test tool",
            llm_responses=[
                '[{"tool": "tool_execute", args:{"id": "' + uuid + '", "kwargs": {}}}]'
            ],
            expected_content_contains="EXECUTED CORRECTLY."
        )
    ]
