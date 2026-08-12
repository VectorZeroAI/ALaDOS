#!/usr/bin/env python3
"""Executor-core state-machine tests.

These tests deliberately mock *tool implementation* at the execute_tool
boundary.  Builtin/syscall behavior is tested by builtins_test.py and
lib_test.py; core_test.py is responsible for proving that the executor moves
between API, execute, recovery, paradox, and finish states correctly.
"""
from __future__ import annotations

import json
import queue
import threading
import time

import flask
import httpx
import pytest
from werkzeug.serving import make_server

import python.executor.main as executor_main
from python.executor.exceptions import ParadoxDetected
from python.executor.types import Api
from python.utils.conn_factory import conn_factory


class LLMMockServer:
    def __init__(self) -> None:
        self.app = flask.Flask(__name__)
        self.responses: queue.Queue[str] = queue.Queue()
        self.server = None
        self.thread = None

        @self.app.post("/v1/chat/completions")
        def chat_completions():
            try:
                content = self.responses.get(timeout=10)
            except queue.Empty:
                return flask.jsonify({"error": "No mock response left"}), 500
            return flask.jsonify({"choices": [{"message": {"content": content}}]})

    def start(self) -> None:
        self.server = make_server("127.0.0.1", 0, self.app)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                if httpx.get(
                    f"http://127.0.0.1:{self.server.server_port}/",
                    timeout=0.2,
                ).status_code == 404:
                    return
            except httpx.RequestError:
                time.sleep(0.05)
        raise RuntimeError("mock LLM server failed to start")

    @property
    def port(self) -> int:
        assert self.server is not None
        return self.server.server_port

    def put(self, response: str) -> None:
        self.responses.put(response)

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
        if self.thread is not None:
            self.thread.join(timeout=5)


class StopCore(Exception):
    """Private sentinel used to stop the otherwise infinite executor loop."""


class OneShotQueue:
    def __init__(self, slave_addr: int):
        self.slave_addr = slave_addr
        self.used = False

    def get(self):
        if self.used:
            raise StopCore
        self.used = True
        return self.slave_addr


def run_core_once(slave_addr: int, api: Api) -> threading.Thread:
    q = OneShotQueue(slave_addr)

    def runner() -> None:
        try:
            executor_main.core(q, [api])
        except StopCore:
            pass

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return thread


@pytest.fixture
def core_env(monkeypatch):
    mock = LLMMockServer()
    mock.start()

    api = Api(
        url=f"http://127.0.0.1:{mock.port}/v1/chat/completions",
        key="test",
        model="mock",
        max_tokens=8000,
    )

    # Core constructs _ExecToolMetaData with a NATS client even when the test
    # replaces execute_tool.  No network connection is needed at this layer.
    async def fake_connect_nats():
        return None

    monkeypatch.setattr(executor_main, "connect_nats", fake_connect_nats)
    yield mock, api
    mock.stop()


@pytest.fixture
def core_db():
    conn = conn_factory("alados_test")
    conn.autocommit = True
    clean_database(conn)
    try:
        yield conn
    finally:
        clean_database(conn)
        conn.close()


def clean_database(conn) -> None:
    """Remove all test-created rows without violating RESTRICT FKs."""
    tables = [
        "event_call_fill_result",
        "event_call_execute_slave",
        "event_call_rmt",
        "event_consumers",
        "master_req",
        "slave_req",
        "master_load",
        "master_context",
        "rmt_slaves",
        "cronjob_once",
        "cronjob_loop",
        "reusable_master_templates",
        "executables",
        "knowledge",
        "vector_ops",
        "names",
        "slaves",
        "masters",
        "results",
    ]
    with conn.transaction():
        for table in tables:
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM addrs WHERE addr > 0")
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")


def new_slave(db, instruction: str) -> tuple[int, int]:
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(NULL, %s, NULL, NULL, NULL, NULL, NULL, 'general')",
        (instruction,),
    )
    result_addr = db.execute_fetchval(
        "SELECT result_addr FROM slaves WHERE addr = %s",
        (slave_addr,),
    )
    return slave_addr, result_addr


def wait_for_result(db, result_addr: int, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = db.execute(
            "SELECT ready, status, content_str FROM results WHERE addr = %s",
            (result_addr,),
        ).fetchone()
        if row is None:
            raise AssertionError(f"Result {result_addr} disappeared")
        ready, status, content = row
        if ready:
            return content, status
        if status == "error":
            return content, status
        time.sleep(0.05)
    row = db.execute(
        "SELECT ready, status, content_str FROM results WHERE addr = %s",
        (result_addr,),
    ).fetchone()
    raise TimeoutError(f"Result {result_addr} not ready; state={row!r}")


def install_execute(monkeypatch, db, behavior):
    """Patch the exact execute_tool symbol used by core."""
    monkeypatch.setattr(executor_main, "execute_tool", behavior)


def run_case(core_db, core_env, monkeypatch, instruction, responses, behavior):
    mock, api = core_env
    slave_addr, result_addr = new_slave(core_db, instruction)
    for response in responses:
        mock.put(response)
    install_execute(monkeypatch, core_db, behavior)
    thread = run_core_once(slave_addr, api)
    result = wait_for_result(core_db, result_addr)
    thread.join(timeout=2)
    assert not thread.is_alive()
    return result


class TestBasicExecution:
    def test_writes_final_result(self, core_db, core_env, monkeypatch):
        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            return "Hello World"

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Write 'Hello World'",
            ['[{"tool":"result_write","args":{"text":"Hello World"}}]'],
            execute,
        )
        assert result == ("Hello World", None)
        assert calls == ["result_write"]


class TestParadoxHandling:
    def test_paradox_path_returns_to_normal_execution(self, core_db, core_env, monkeypatch):
        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            if len(calls) == 1:
                raise ParadoxDetected("test paradox", [meta.slave_id])
            return "done!"

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Report a paradox",
            [
                '[{"tool":"k_report_paradoxal_information","args":{"items":[],"paradox":"test"}}]',
                '[{"tool":"result_write","args":{"text":"assume handled."}}]',
                '[{"tool":"result_write","args":{"text":"done!"}}]',
            ],
            execute,
        )
        assert result == ("done!", None)
        assert len(calls) >= 2


class TestErrorRecovery:
    def test_failed_tool_is_replaced_by_llm_recovery_call(self, core_db, core_env, monkeypatch):
        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            if len(calls) == 1:
                raise RuntimeError("synthetic failure")
            return "recovered"

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Fail then recover",
            [
                '[{"tool":"broken_tool","args":{}}]',
                '[{"tool":"recovered_tool","args":{}}]',
            ],
            execute,
        )
        assert result == ("recovered", None)
        assert calls == ["broken_tool", "recovered_tool"]


class TestCreateAndReadKnowledge:
    def test_multiple_tool_calls_are_finished_as_one_execution(self, core_db, core_env, monkeypatch):
        name = "core_knowledge_test"
        state = {}

        def execute(call, meta):
            if call.tool == "k_create":
                state[name] = "moon is cheese"
                return "created"
            if call.tool == "k_read":
                return state[name]
            raise AssertionError(call.tool)

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Create and read knowledge",
            [
                json.dumps([
                    {"tool": "k_create", "args": {"name": name}},
                    {"tool": "k_read", "args": {"id": name}},
                ])
            ],
            execute,
        )
        assert result == ("created\nmoon is cheese", None)
        assert state[name] == "moon is cheese"


class TestNestedNewPaths:
    def test_tool_error_recovery_does_not_require_scheduler(self, core_db, core_env, monkeypatch):
        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            if call.tool == "NOT_EXISTS":
                raise RuntimeError("unknown tool")
            return "recovered tool_error"

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Start wrongly",
            [
                '[{"tool":"NOT_EXISTS","args":{}}]',
                '[{"tool":"result_write","args":{"text":"recovered tool_error"}}]',
            ],
            execute,
        )
        assert result == ("recovered tool_error", None)
        assert calls == ["NOT_EXISTS", "result_write"]


class TestToolExecuteBuiltinFunc:
    def test_tool_create_then_execute_calls_are_processed_in_order(self, core_db, core_env, monkeypatch):
        created = False
        calls = []

        def execute(call, meta):
            nonlocal created
            calls.append(call.tool)
            if call.tool == "tool_create":
                created = True
                return "created"
            if call.tool == "tool_execute":
                assert created
                return "EXECUTED CORRECTLY."
            raise AssertionError(call.tool)

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Create and use a test tool",
            [json.dumps([
                {"tool": "tool_create", "args": {"name": "core_test_tool"}},
                {"tool": "tool_execute", "args": {"id": "core_test_tool", "kwargs": {}}},
            ])],
            execute,
        )
        assert result == ("created\nEXECUTED CORRECTLY.", None)
        assert calls == ["tool_create", "tool_execute"]


class TestFatalRecovery:
    def test_repeated_tool_failures_end_in_error_result(self, core_db, core_env, monkeypatch):
        def execute(call, meta):
            raise RuntimeError("always broken")

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Always fail",
            [
                '[{"tool":"broken","args":{}}]',
                '[{"tool":"broken_again","args":{}}]',
            ],
            execute,
        )
        assert result[1] == "error"


class TestCoreIsolation:
    def test_core_uses_direct_queue_without_scheduler_startup(self, core_db, core_env, monkeypatch):
        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            return "isolated"

        result = run_case(
            core_db,
            core_env,
            monkeypatch,
            "Direct queue execution",
            ['[{"tool":"test_tool","args":{}}]'],
            execute,
        )
        assert result == ("isolated", None)
        assert calls == ["test_tool"]
