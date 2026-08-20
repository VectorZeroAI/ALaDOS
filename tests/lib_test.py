#!/usr/bin/env python3
"""
Integration tests for the ALaDOS syscall client library.

The dispatcher intentionally runs in its own thread and uses its own psycopg
connection. Sharing the test connection across threads was the source of the
NATS timeouts in the old suite.
"""
from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime

import nats
import pytest

import ALaDOS.lib._.main as lib_main
from ALaDOS.lib import Context, Event, Executables, Goal, Knowledge, Report, Result
from ALaDOS.lib._.main import batch_call, syscall as SyscallSpec
from python.executor.execute_tool import execute_syscall
from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import _ExecToolMetaData
from python.types import SysCall
from python.utils.conn_factory import conn_factory

from .conftest import unique_name


TEST_DB_NAME = "alados_test"


def clean_database(conn):
    """Remove all test-created rows without violating RESTRICT FKs."""
    tables = [
        "event_call_fill_result", "event_call_execute_slave", "event_call_rmt",
        "event_consumers",
        "master_req", "slave_req", "master_load", "master_context",
        "rmt_slaves",
        "cronjob_once", "cronjob_loop",
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
            conn.execute(f"DELETE FROM {table}")
        conn.execute("ALTER SEQUENCE global_next_id RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_planner_serial RESTART WITH 1")
        conn.execute("ALTER SEQUENCE global_rmt_activation_serial RESTART WITH 1")


@pytest.fixture
def lib_db():
    conn = conn_factory(TEST_DB_NAME)
    conn.autocommit = True
    clean_database(conn)
    yield conn
    clean_database(conn)
    conn.close()


class _Dispatcher:
    """Real NATS syscall endpoint used by the library integration tests."""

    def __init__(self):
        self._loop = None
        self._thread = None
        self._nc = None
        self._db = None
        self._ready = threading.Event()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("dispatcher failed to start")

    def stop(self):
        if self._loop is not None and self._nc is not None:
            future = asyncio.run_coroutine_threadsafe(self._nc.drain(), self._loop)
            try:
                future.result(timeout=5)
            except Exception:
                future = asyncio.run_coroutine_threadsafe(self._nc.close(), self._loop)
                try:
                    future.result(timeout=5)
                except Exception:
                    pass
        if self._thread is not None:
            self._thread.join(timeout=5)
            assert not self._thread.is_alive()

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main())
        finally:
            self._loop.close()

    async def _main(self):
        self._db = conn_factory(TEST_DB_NAME)
        self._db.autocommit = True
        nt = await nats.connect()
        self._nc = nt

        async def handler(msg):
            await self._handle(msg, nt)

        await nt.subscribe("_.syscall.*.*", cb=handler)
        await nt.flush()
        self._ready.set()

        try:
            while not nt.is_closed:
                await asyncio.sleep(0.05)
        finally:
            if not nt.is_closed:
                await nt.close()
            if self._db is not None:
                self._db.close()

    async def _handle(self, msg, nt):
        parts = msg.subject.split(".", 3)
        slave_addr = int(parts[-2])
        tool_name = parts[-1]

        try:
            assert self._db is not None
            master_addr = self._db.execute_fetchval(
                "SELECT master_addr FROM slaves WHERE addr = %s",
                (slave_addr,),
            ) or 0
            args = json.loads(msg.data.decode())
            meta = _ExecToolMetaData(
                master_addr=master_addr,
                conn=self._db,
                slave_addr=slave_addr,
                context_limit=40000,
                occ_last_change=datetime.now(),
                syscalls_queue=syscalls_queue_dict_per_slave[slave_addr],
                nats=nt,
            )
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                execute_syscall,
                SysCall(called_id=tool_name, args=args),
                meta,
            )
        except Exception as e:
            result = f"__DISPATCH_ERROR__:{type(e).__name__}: {e}"

        if msg.reply and not nt.is_closed:
            await msg.respond(str(result).encode())


@pytest.fixture
def dispatcher(lib_db, monkeypatch):
    d = _Dispatcher()
    d.start()
    yield d
    d.stop()


@pytest.fixture
def slave(lib_db):
    master_addr = lib_db.execute_fetchval("SELECT new_master('lib_test_master')")
    slave_addr = lib_db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', %s, NULL, NULL, NULL, NULL, 'general')",
        (master_addr, unique_name("lib_slave")),
    )
    return slave_addr


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
async def reset_lib_nats():
    lib_main.nt = None
    yield
    nt = lib_main.nt
    lib_main.nt = None
    if nt is not None and not nt.is_closed:
        await nt.close()


class TestBatchCall:
    @pytest.mark.anyio
    async def test_batch_call_subject_reaches_dispatcher(self, dispatcher, slave, lib_db):
        nt = await nats.connect()
        try:
            name = unique_name("batchsanity")
            reply = await nt.request(
                f"_.syscall.{slave}.k_create",
                json.dumps({"content": "c", "description": "d", "name": name}).encode(),
                timeout=5,
            )
            addr = int(reply.data.decode())
            assert addr > 0
            assert lib_db.execute_fetchval(
                "SELECT addr FROM names WHERE name = %s", (name,)
            ) == addr
        finally:
            await nt.close()

    @pytest.mark.anyio
    async def test_batch_call_returns_results_in_order(self, dispatcher, slave, lib_db):
        name_a = unique_name("batch_a")
        name_b = unique_name("batch_b")
        calls = [
            SyscallSpec("k_create", {"content": "ca", "description": "da", "name": name_a}),
            SyscallSpec("k_create", {"content": "cb", "description": "db", "name": name_b}),
        ]
        results = await batch_call(calls, slave)
        assert len(results) == 2
        addr_a = int(results[0])
        addr_b = int(results[1])
        assert addr_a == lib_db.execute_fetchval(
            "SELECT addr FROM names WHERE name = %s", (name_a,)
        )
        assert addr_b == lib_db.execute_fetchval(
            "SELECT addr FROM names WHERE name = %s", (name_b,)
        )


class TestKnowledgeLib:
    @pytest.mark.anyio
    async def test_create_and_read_round_trip(self, dispatcher, slave, lib_db):
        name = unique_name("kn")
        addr = await Knowledge.create(slave, content="hello", description="desc", name=name)
        assert addr == lib_db.execute_fetchval("SELECT addr FROM names WHERE name = %s", (name,))
        content = await Knowledge.read(name, slave)
        assert content == "hello"

    @pytest.mark.anyio
    async def test_edit_updates_content(self, dispatcher, slave):
        name = unique_name("kn_edit")
        await Knowledge.create(slave, content="old text", description="desc", name=name)
        # The syscall executes in its own committed transaction; the OCC test
        # should compare against the item's actual stored timestamp.
        await Knowledge.edit(
            name, slave,
            content_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>",
            description_change=None,
        )
        content = await Knowledge.read(name, slave)
        assert content == "new text"


class TestExecutablesLib:
    @pytest.mark.anyio
    async def test_create_registers_a_new_tool(self, dispatcher, slave, lib_db):
        name = unique_name("tool")
        addr = await Executables.create(
            slave,
            description="a test tool",
            header="def f(): pass",
            body="print('hi')",
            name=name,
        )
        assert addr == lib_db.execute_fetchval("SELECT addr FROM names WHERE name = %s", (name,))
        body = lib_db.execute_fetchval("SELECT body FROM executables WHERE addr = %s", (addr,))
        assert body == "print('hi')"

    @pytest.mark.anyio
    async def test_execute_runs_created_tool_and_returns_output(self, dispatcher, slave):
        name = unique_name("runtool")
        await Executables.create(
            slave,
            description="prints a marker",
            header="",
            body="print('EXECUTABLES_LIB_TEST_OK')",
            name=name,
        )
        output = await Executables.execute(slave, id=name, timeout=10)
        assert "EXECUTABLES_LIB_TEST_OK" in output


class TestContextLib:
    @pytest.mark.anyio
    async def test_add_loads_item_into_master_context(self, dispatcher, slave, lib_db):
        name = unique_name("ctx")
        addr = await Knowledge.create(slave, content="ctx content", description="d", name=name)
        await Context.add(slave, id=addr)
        master_addr = lib_db.execute_fetchval("SELECT master_addr FROM slaves WHERE addr = %s", (slave,))
        loaded = lib_db.execute(
            "SELECT item_addr FROM master_load WHERE master_addr = %s", (master_addr,)
        ).fetchall()
        assert addr in [row[0] for row in loaded]

    @pytest.mark.anyio
    async def test_window_change_size_returns_new_sizes(self, dispatcher, slave):
        result = await Context.window_change_size(slave, left=2, right=3)
        assert result["left"] == 2
        assert result["right"] == 3


class TestGoalLib:
    @pytest.mark.anyio
    async def test_add_slave_creates_new_slave_row(self, dispatcher, slave, lib_db):
        name = unique_name("newslave")
        addr = await Goal.add_slave(slave, instruction="do something", slave_name=name)
        assert addr == lib_db.execute_fetchval("SELECT addr FROM names WHERE name = %s", (name,))
        instruction = lib_db.execute_fetchval("SELECT instruction FROM slaves WHERE addr = %s", (addr,))
        assert instruction == "do something"

    @pytest.mark.anyio
    async def test_add_master_creates_new_master_row(self, dispatcher, slave, lib_db):
        addr = await Goal.add_master(slave, instruction="do something else")
        instruction = lib_db.execute_fetchval("SELECT instruction FROM masters WHERE addr = %s", (addr,))
        assert instruction == "do something else"


class TestResultLib:
    @pytest.mark.anyio
    async def test_write_returns_same_text(self, dispatcher, slave):
        result = await Result.write(slave, text="the result")
        assert result == "the result"

    @pytest.mark.anyio
    async def test_add_master_result_appends_to_master_result(self, dispatcher, slave, lib_db):
        master_addr = lib_db.execute_fetchval("SELECT master_addr FROM slaves WHERE addr = %s", (slave,))
        result_addr = lib_db.execute_fetchval("SELECT result_addr FROM masters WHERE addr = %s", (master_addr,))
        await Result.add_master_result(slave, text="more result text")
        content = lib_db.execute_fetchval("SELECT content_str FROM results WHERE addr = %s", (result_addr,))
        assert "more result text" in content


class TestEventLib:
    @pytest.mark.anyio
    async def test_create_result_returns_result_and_consumer_addrs(self, dispatcher, slave):
        result = await Event.create_result(slave, event_path="evt.some.path", result_str="got ${{data}}")
        assert "result_addr" in result
        assert "consumer_addr" in result
        assert result["result_addr"] > 0
        assert result["consumer_addr"] > 0

    @pytest.mark.anyio
    async def test_register_reaction_slave_creates_consumer_row(self, dispatcher, slave, lib_db):
        consumer_addr = await Event.register_reaction_slave(slave, event_path="evt.some.path", instruction="react", scope="general")
        event_path = lib_db.execute_fetchval("SELECT event_path FROM event_consumers WHERE addr = %s", (consumer_addr,))
        assert event_path == "evt.some.path"



class TestDispatcherSubjectParsing:
    @pytest.mark.anyio
    async def test_dispatcher_rejects_unknown_tool_name(self, dispatcher, slave):
        nt = await nats.connect()
        try:
            reply = await nt.request(
                f"_.syscall.{slave}.not_a_real_tool",
                b"{}",
                timeout=5,
            )
            assert reply.data.decode().startswith("__DISPATCH_ERROR__:")
        finally:
            await nt.close()

