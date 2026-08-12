#!/usr/bin/env python3
"""
Focused executor-core integration tests.

These tests exercise the executor state machine itself. Tool implementation and
syscall transport are covered by builtins_test.py and lib_test.py respectively.
Keeping those layers separate avoids booting the full scheduler/event system and
also keeps the suite small enough for constrained devices.
"""
from __future__ import annotations

import queue
import threading
import time

import flask
import httpx
import pytest
from werkzeug.serving import make_server

import python.executor.main as executor_main
from python.executor.exceptions import ParadoxDetected
from python.executor.queue import embedder_queue
from python.executor.types import Api
from python.utils.conn_factory import conn_factory


class LLMMockServer:
    def __init__(self) -> None:
        self.app = flask.Flask(__name__)
        self.response_queue: queue.Queue[str] = queue.Queue()
        self.server = None
        self.thread = None

        @self.app.route("/v1/chat/completions", methods=["POST"])
        def chat_completions():
            try:
                content = self.response_queue.get(timeout=5)
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
                response = httpx.get(
                    f"http://127.0.0.1:{self.server.server_port}/",
                    timeout=0.2,
                )
                if response.status_code == 404:
                    return
            except httpx.RequestError:
                time.sleep(0.05)
        raise RuntimeError("mock LLM server failed to start")

    @property
    def port(self) -> int:
        assert self.server is not None
        return self.server.server_port

    def put(self, response: str) -> None:
        self.response_queue.put(response)

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
        if self.thread is not None:
            self.thread.join(timeout=5)


class StopCore(Exception):
    pass


class OneShotQueue:
    """Queue with exactly one task; the executor exits after processing it."""
    def __init__(self, item: int):
        self.item = item
        self.used = False

    def get(self):
        if not self.used:
            self.used = True
            return self.item
        raise StopCore


def run_core_once(task_queue: OneShotQueue, api: Api) -> threading.Thread:
    def runner() -> None:
        try:
            executor_main.core(task_queue, [api])
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

    # The core constructs NATS metadata in ExecuteState, but the focused tests
    # replace tool execution itself, so no NATS service is required here.
    async def fake_connect_nats():
        return object()

    monkeypatch.setattr(executor_main, "connect_nats", fake_connect_nats)
    monkeypatch.setattr(executor_main, "embedder_queue", embedder_queue)

    yield mock, api

    mock.stop()


@pytest.fixture
def core_db():
    conn = conn_factory("alados_test")
    conn.autocommit = True

    tables = [
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves", "reusable_master_templates",
        "slaves", "masters", "results", "names", "vector_ops",
        "executables", "knowledge",
        "cronjob_once", "cronjob_loop",
        "event_consumers", "event_call_rmt",
        "event_call_execute_slave", "event_call_fill_result",
    ]
    for table in tables:
        conn.execute(f"DELETE FROM {table} CASCADE")
    conn.execute("DELETE FROM addrs WHERE addr > 0")
    conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
    conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
    conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")
    yield conn
    for table in tables:
        conn.execute(f"DELETE FROM {table} CASCADE")
    conn.execute("DELETE FROM addrs WHERE addr > 0")
    conn.close()


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
            return content
        if status == "error":
            raise AssertionError(f"Result {result_addr} entered error state")
        time.sleep(0.05)

    row = db.execute(
        "SELECT ready, status, content_str FROM results WHERE addr = %s",
        (result_addr,),
    ).fetchone()
    raise TimeoutError(f"Result {result_addr} not ready; state={row!r}")


def stop_after_finish(task_queue: queue.Queue[int], slave_addr: int) -> None:
    task_queue.put(slave_addr)


class TestCoreExecution:
    def test_basic_execution_finishes(self, core_db, core_env, monkeypatch):
        mock, api = core_env
        slave_addr, result_addr = new_slave(core_db, "Write something")

        monkeypatch.setattr(
            executor_main,
            "execute_tool",
            lambda call, meta: "Hello World",
        )
        mock.put('[{"tool": "fake_tool", "args": {}}]')

        thread = run_core_once(OneShotQueue(slave_addr), api)
        content = wait_for_result(core_db, result_addr)

        assert content == "Hello World"

        # Let the core consume the next GetSlaveState and terminate.
        thread.join(timeout=2)
        assert not thread.is_alive()

    def test_error_recovery(self, core_db, core_env, monkeypatch):
        mock, api = core_env
        slave_addr, result_addr = new_slave(core_db, "Recover from a failed tool")

        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            if len(calls) == 1:
                raise RuntimeError("synthetic failure")
            return "recovered"

        monkeypatch.setattr(executor_main, "execute_tool", execute)

        mock.put('[{"tool": "broken_tool", "args": {}}]')
        mock.put('[{"tool": "recovered_tool", "args": {}}]')

        thread = run_core_once(OneShotQueue(slave_addr), api)
        assert wait_for_result(core_db, result_addr) == "recovered"
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert calls == ["broken_tool", "recovered_tool"]

    def test_paradox_recovery_path(self, core_db, core_env, monkeypatch):
        mock, api = core_env
        slave_addr, result_addr = new_slave(core_db, "Resolve a paradox")

        # Give ParadoxState one real knowledge item to resolve/load.
        item_addr = core_db.execute_fetchval("SELECT new_addr()")
        core_db.execute(
            "INSERT INTO knowledge(addr, content) VALUES (%s, %s)",
            (item_addr, "test paradox item"),
        )
        core_db.execute(
            "INSERT INTO vector_ops(addr_k, description) VALUES (%s, %s)",
            (item_addr, "test"),
        )

        calls = []

        def execute(call, meta):
            calls.append(call.tool)
            if len(calls) == 1:
                raise ParadoxDetected("conflicting facts", [item_addr])
            return "resolved"

        monkeypatch.setattr(executor_main, "execute_tool", execute)

        # ParadoxState -> API/execute (finish=False) -> normal context path ->
        # API/execute (finish=True) -> FinishState.
        mock.put('[{"tool": "resolve_paradox", "args": {}}]')
        mock.put('[{"tool": "paradox_step_complete", "args": {}}]')
        mock.put('[{"tool": "continue_after_paradox", "args": {}}]')

        thread = run_core_once(OneShotQueue(slave_addr), api)
        assert wait_for_result(core_db, result_addr) == "resolved"
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert calls == ["resolve_paradox", "paradox_step_complete", "continue_after_paradox"]
