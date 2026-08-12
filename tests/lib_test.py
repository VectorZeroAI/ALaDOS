#!/usr/bin/env python3
"""
Integration test suite for the `lib` package.
"""

import asyncio
import json
import threading
from datetime import datetime
from unittest.mock import patch

import nats
import pytest
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from ALaDOS.lib import Context, Event, Executables, Goal, Knowledge, Report, Result
from ALaDOS.lib._.main import batch_call, syscall as SyscallSpec
from python.executor.execute_tool import execute_syscall
from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import _ExecToolMetaData
from python.types import ToolCall
from python.utils.name_resolver import resolve_to_addr

from conftest import db, meta, unique_name  # noqa: F401


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Dispatcher:
    def __init__(self, db_conn):
        self._db = db_conn
        self._loop = None
        self._thread = None
        self._ready = threading.Event()
        self._stop = threading.Event()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("dispatcher failed to start")

    def stop(self):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main())

    async def _main(self):
        nt = await nats.connect()
        sub = await nt.subscribe("_.syscall.*.*")
        self._ready.set()
        try:
            async for msg in sub.messages:
                await self._handle(msg, nt)
                if self._stop.is_set():
                    break
        finally:
            await sub.unsubscribe()
            await nt.close()

    async def _handle(self, msg, nt):
        parts = msg.subject.split(".", 3)
        slave_addr = int(parts[-2])
        tool_name = parts[-1]
        try:
            meta = _ExecToolMetaData(
                master_id=0,
                conn=self._db,
                slave_id=slave_addr,
                context_limit=40000,
                occ_last_change=datetime.now(),
                syscalls_queue=syscalls_queue_dict_per_slave[slave_addr],
                nats=nt,
            )
            args = json.loads(msg.data.decode())
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, execute_syscall, ToolCall(tool=tool_name, args=args), meta
            )
        except Exception as e:
            result = f"__DISPATCH_ERROR__:{e}"
        if msg.reply:
            await msg.respond(str(result).encode())


@pytest.fixture
def dispatcher(db):
    d = _Dispatcher(db)
    d.start()
    yield d
    d.stop()


@pytest.fixture
def slave(db):
    master_addr = db.execute_fetchval("SELECT new_master('lib_test_master')")
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', 'dummy_slave', NULL, NULL, NULL, NULL, 'general')",
        (master_addr,),
    )
    return slave_addr


class TestBatchCall:
    @pytest.mark.anyio
    async def test_batch_call_subject_reaches_dispatcher(self, dispatcher, slave, db):
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
            assert resolve_to_addr(name, db) == addr
        finally:
            await nt.close()

    @pytest.mark.anyio
    async def test_batch_call_returns_results_in_order(self, dispatcher, slave, db):
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
        assert addr_a == resolve_to_addr(name_a, db)
        assert addr_b == resolve_to_addr(name_b, db)

    def test_batch_call_reply_attribute_bug(self):
        import inspect
        from ALaDOS.lib._ import main as transport_main
        source = inspect.getsource(transport_main.batch_call)
        assert ".data" in source, (
            "batch_call should read response payloads via Msg.data, not Msg.reply"
        )


class TestKnowledgeLib:
    @pytest.mark.anyio
    async def test_create_and_read_round_trip(self, dispatcher, slave, db):
        name = unique_name("kn")
        addr = await Knowledge.create(slave, content="hello", description="desc", name=name)
        assert addr == resolve_to_addr(name, db)
        content = await Knowledge.read(name, slave)
        assert content == "hello"

    @pytest.mark.anyio
    async def test_edit_updates_content(self, dispatcher, slave, db):
        name = unique_name("kn_edit")
        await Knowledge.create(slave, content="old text", description="desc", name=name)
        await Knowledge.edit(
            name, slave,
            content_change="<SEARCH>old</SEARCH><REPLACE>new</REPLACE>",
            description_change=None,
        )
        content = await Knowledge.read(name, slave)
        assert content == "new text"


class TestExecutablesLib:
    @pytest.mark.anyio
    async def test_create_registers_a_new_tool(self, dispatcher, slave, db):
        name = unique_name("tool")
        addr = await Executables.create(slave, description="a test tool", header="def f(): pass", body="print('hi')", name=name)
        assert addr == resolve_to_addr(name, db)
        body = db.execute_fetchval("SELECT body FROM executables WHERE addr = %s", (addr,))
        assert body == "print('hi')"

    @pytest.mark.anyio
    async def test_execute_runs_created_tool_and_returns_output(self, dispatcher, slave, db):
        name = unique_name("runtool")
        await Executables.create(slave, description="prints a marker", header="", body="print('EXECUTABLES_LIB_TEST_OK')", name=name)
        output = await Executables.execute(slave, id=name, timeout=10)
        assert "EXECUTABLES_LIB_TEST_OK" in output


class TestContextLib:
    @pytest.mark.anyio
    async def test_add_loads_item_into_master_context(self, dispatcher, slave, db):
        name = unique_name("ctx")
        addr = await Knowledge.create(slave, content="ctx content", description="d", name=name)
        await Context.add(slave, id=addr)
        master_addr = db.execute_fetchval("SELECT master_addr FROM slaves WHERE addr = %s", (slave,))
        loaded = db.execute("SELECT addr FROM master_context WHERE master_addr = %s", (master_addr,)).fetchall()
        assert addr in [row[0] for row in loaded]

    @pytest.mark.anyio
    async def test_window_change_size_returns_new_sizes(self, dispatcher, slave):
        result = await Context.window_change_size(slave, left=2, right=3)
        assert result["left"] == 2
        assert result["right"] == 3


class TestGoalLib:
    @pytest.mark.anyio
    async def test_add_slave_creates_new_slave_row(self, dispatcher, slave, db):
        name = unique_name("newslave")
        addr = await Goal.add_slave(slave, instruction="do something", slave_name=name)
        assert addr == resolve_to_addr(name, db)
        instruction = db.execute_fetchval("SELECT instruction FROM slaves WHERE addr = %s", (addr,))
        assert instruction == "do something"

    @pytest.mark.anyio
    async def test_add_master_creates_new_master_row(self, dispatcher, slave, db):
        addr = await Goal.add_master(slave, instruction="do something else")
        instruction = db.execute_fetchval("SELECT instruction FROM masters WHERE addr = %s", (addr,))
        assert instruction == "do something else"


class TestResultLib:
    @pytest.mark.anyio
    async def test_write_returns_same_text(self, dispatcher, slave):
        result = await Result.write(slave, text="the result")
        assert result == "the result"

    @pytest.mark.anyio
    async def test_add_master_result_appends_to_master_result(self, dispatcher, slave, db):
        master_addr = db.execute_fetchval("SELECT master_addr FROM slaves WHERE addr = %s", (slave,))
        result_addr = db.execute_fetchval("SELECT result_addr FROM masters WHERE addr = %s", (master_addr,))
        await Result.add_master_result(slave, text="more result text")
        content = db.execute_fetchval("SELECT content_str FROM results WHERE addr = %s", (result_addr,))
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
    async def test_register_reaction_slave_creates_consumer_row(self, dispatcher, slave, db):
        consumer_addr = await Event.register_reaction_slave(slave, event_path="evt.some.path", instruction="react", scope="general")
        event_path = db.execute_fetchval("SELECT event_path FROM event_consumers WHERE addr = %s", (consumer_addr,))
        assert event_path == "evt.some.path"


class TestReportLib:
    @pytest.mark.anyio
    async def test_report_paradoxal_information_completes_without_raising(self, dispatcher, slave):
        result = await Report.report_paradoxal_information(slave, items=[1, 2], paradox="conflicting facts")
        assert result is None


class TestDispatcherSubjectParsing:
    @pytest.mark.anyio
    async def test_dispatcher_resolves_correct_slave_and_tool_from_subject(self, dispatcher, slave, db):
        nt = await nats.connect()
        try:
            name = unique_name("dispatchcheck")
            reply = await nt.request(
                f"_.syscall.{slave}.k_create",
                json.dumps({"content": "c2", "description": "d2", "name": name}).encode(),
                timeout=5,
            )
            addr = int(reply.data.decode())
            assert resolve_to_addr(name, db) == addr
        finally:
            await nt.close()

    @pytest.mark.anyio
    async def test_dispatcher_rejects_unknown_tool_name(self, dispatcher, slave):
        nt = await nats.connect()
        try:
            reply = await nt.request(f"_.syscall.{slave}.not_a_real_tool", b"{}", timeout=5)
            assert reply.data.decode().startswith("__DISPATCH_ERROR__:")
        finally:
            await nt.close()
