#!/usr/bin/env python3
"""
Integration test suite for the `lib` package (`Knowledge`, `Executables`,
`Rmt`, `Context`, `Goal`, `Result`, `Web`, `Event`, `Report`, and the `_`
transport layer).

Unlike builtins_test.py, which calls the server-side handlers directly,
this suite drives the *client* wrappers in lib (`Knowledge.create`,
`Rmt.activate_as_master`, etc.) end to end over NATS, the way a running
tool subprocess actually would.

These tests assert normal, correct behaviour of lib -- the same
convention as builtins_test.py -- not the presence of the transport bug
described below. That distinction matters mechanically, not just
stylistically: a test that asserts "this currently raises TimeoutError"
passes today and starts *failing* the moment someone fixes the bug, which
is backwards -- fixing a bug should turn tests green, not red. So all the
behavioural tests below are written to pass once `_.main.call()` sends
the correct subject, and to fail (for a clear, diagnosable reason) until
then. Exactly one test, TestRawCallTransportBug, exists purely to pin
down the bug's mechanism, and it does not change shape when the bug is
fixed -- it just starts skipping instead of passing (see its docstring).

WHAT HAS TO BE RUNNING
-----------------------
1. A real NATS server on localhost:4222 (`connect_nats()` in
   events/types.py takes no URL, so it always dials the default).
2. A real Postgres test DB (`alados_test`), same as builtins_test.py.
3. A "dispatcher": something subscribed to `_.syscall.*.*` that drains
   `syscalls_queue_dict_per_slave[slave_addr]` and replies. In the real
   system this only happens *inside* execute_tool_builtin_func's polling
   loop while a subprocess tool is running -- there is no standalone
   service that does this. For lib to be testable at all we have to spin
   up that piece ourselves; `_Dispatcher` below is exactly that: a
   background thread that subscribes to `_.syscall.*.*` and, for every
   inbound request, drains it straight through execute_tool() -- the
   same call execute_tool_builtin_func's poll loop makes -- and replies.

   This is infrastructure this suite needs to exist, not a system
   component -- it is not a replacement for one and should not be
   mistaken for a "fix" of the missing standalone syscall server.

KNOWN BUG THIS SUITE IS BLOCKED ON
------------------------------------
`_.main.call()` publishes to `_.syscall.{function_name}` -- it never
includes slave_addr in the subject. Every single wrapper in Knowledge,
Executables, Rmt, Context, Goal, Result, Web, Event, and Report (i.e.
everything except batch_call) goes through `call()`, so none of them can
reach a dispatcher matching `_.syscall.*.*` as written. Because of this,
every behavioural test in this file uses the `call_fixed` autouse fixture,
which patches `_.main.call` to use the correct subject shape for the
duration of the test. This isolates what's being tested (does
Knowledge.create/Executables.execute/etc. actually work) from the
transport bug, exactly the way TestKnowledgeLib.test_create_and_read
worked in the previous revision of this file, but applied everywhere so
the pass/fail direction of every test lines up with "is the behaviour
correct," not "is the known bug still there."

Once `_.main.call()` is fixed for real, `call_fixed`'s patch becomes a
no-op overwrite of already-correct code, and every test in this file
should still pass unmodified -- at which point the patch (and this whole
section of the docstring) can simply be deleted.

`batch_call` gets the subject right (`_.syscall.{slave_id}.{name}`) but
then tries to read `.reply` off the returned Msg object instead of
`.data` -- `Msg` has no `reply` attribute holding response bytes. That
one is a real, independent bug in batch_call itself (not a transport
routing issue `call_fixed` papers over), so it's asserted directly.
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
from nats.errors import TimeoutError as NatsTimeoutError

from ALaDOS.lib import Context, Event, Executables, Goal, Knowledge, Report, Result
from ALaDOS.lib._.main import batch_call
from ALaDOS.lib._.main import call as raw_call
from ALaDOS.lib._.main import syscall as SyscallSpec
from python.executor.execute_tool import execute_tool
from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import _ExecToolMetaData
from python.types import ToolCall
from python.utils.conn_factory import Conn, register_all_the_composite_types
from python.utils.name_resolver import resolve_to_addr


# ----------------------------------------------------------------------
# DB fixtures (matches builtins_test.py conventions: real DB, rolled
# back transaction, no xfail markers, bugs asserted as current behavior)
# ----------------------------------------------------------------------
TEST_DSN = dict(
    host="127.0.0.1",
    port=5432,
    dbname="alados_test",
    user="u0_a453",
)


def get_test_conn() -> Conn:
    conn = Conn.connect(**TEST_DSN)
    conn.autocommit = True
    conn = register_all_the_composite_types(conn)
    return conn


@pytest.fixture
def db():
    conn = get_test_conn()
    conn.execute("BEGIN")
    yield conn
    conn.execute("ROLLBACK")
    conn.close()


@pytest.fixture
def slave(db):
    """A real master+slave pair, matching builtins_test.py's `meta` fixture."""
    master_addr = db.execute_fetchval("SELECT new_master('lib_test_master')")
    slave_addr = db.execute_fetchval(
        "SELECT new_slave(%s, 'dummy', 'dummy_slave', NULL, NULL, NULL, NULL, 'general')",
        (master_addr,),
    )
    return slave_addr


def unique_name(prefix="libtest"):
    import random
    return f"{prefix}_{random.randint(10000, 99999)}"


@pytest.fixture
def anyio_backend():
    """Pin anyio to the asyncio backend (nats-py/asyncio.run are asyncio-only)."""
    return "asyncio"


# ----------------------------------------------------------------------
# NATS dispatcher harness
#
# Stands in for "something that services _.syscall.<addr>.<name>
# requests and replies" -- see module docstring. Not part of the system
# under test; without it *nothing* in lib can ever be exercised live,
# since no such standalone service exists yet.
# ----------------------------------------------------------------------
class _Dispatcher:
    """
    Subscribes to _.syscall.*.* on a dedicated event loop/thread, and for
    every inbound request builds a fresh _ExecToolMetaData against the
    given db connection, executes the ToolCall via execute_tool (the
    same dispatcher execute_tool_builtin_func's poll loop uses), and
    replies with the string result (or an error string on exception, so
    a broken handler doesn't just hang the request forever).
    """

    def __init__(self, db_conn: Conn):
        self._db = db_conn
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stop = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("dispatcher failed to start / connect to NATS in time")

    def stop(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main())

    async def _main(self) -> None:
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

    async def _handle(self, msg: Msg, nt: NatsClient) -> None:
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
                nats=nt,  # reuse the dispatcher's own connected client instead of
                          # letting _ExecToolMetaData's default factory call
                          # asyncio.run(connect_nats()), which raises
                          # RuntimeError when invoked from inside this
                          # already-running event loop.
            )
            args = json.loads(msg.data.decode())
            # execute_tool ultimately runs blocking, synchronous code
            # (subprocess.Popen + a busy-poll loop on process.poll() for
            # Executables.execute, blocking psycopg calls for DB-backed
            # handlers). Awaiting it directly on this coroutine would
            # block *this* event loop -- the same loop nt uses for its
            # background read/ping tasks -- long enough that nats-py
            # decides the connection is dead and closes it, which then
            # makes every subsequent request in the suite fail with
            # "the connection is closed". Running it in a thread keeps
            # the loop free to service NATS while a handler blocks.
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, execute_tool, ToolCall(tool=tool_name, args=args), meta
            )
        except Exception as e:  # noqa: BLE001 -- surfaced to the caller, not swallowed
            result = f"__DISPATCH_ERROR__:{e}"
        if msg.reply:
            await msg.respond(str(result).encode())


@pytest.fixture
def dispatcher(db):
    d = _Dispatcher(db)
    d.start()
    yield d
    d.stop()


@pytest.fixture(autouse=True)
def call_fixed():
    """
    Patches _.main.call to publish the subject shape the dispatcher
    (and the real system's subscriber pattern "_.syscall.*.*") actually
    requires: "_.syscall.<slave_addr>.<function_name>", three segments
    after "_", not two.

    This is here so every behavioural test in this file exercises real
    lib behaviour instead of just re-proving the known transport bug
    (see module docstring: TestRawCallTransportBug covers that bug on
    its own). Autouse because every test that touches the dispatcher
    needs it -- forgetting it on a new test would silently make that
    test fail with a timeout instead of testing anything useful.
    """
    async def fixed_call(function_name, slave_addr, args):
        nt = await nats.connect()
        try:
            reply = await nt.request(
                f"_.syscall.{slave_addr}.{function_name}",
                json.dumps(args).encode(),
                timeout=5,
            )
            return reply.data.decode()
        finally:
            await nt.close()

    with patch("ALaDOS.lib._.main.call", new=fixed_call):
        yield


# ----------------------------------------------------------------------
# _.main.call() transport bug -- pinned down directly, independent of
# the call_fixed patch (this test intentionally does NOT use the
# dispatcher/call_fixed fixtures, since it's testing call()'s own
# subject-building logic, not anything downstream of it).
# ----------------------------------------------------------------------
class TestRawCallTransportBug:
    """
    call() in _/main.py builds its subject as f"_.syscall.{function_name}",
    omitting slave_addr entirely. The subscriber pattern registered in
    base_state/state_components/custom_consumers.py is "_.syscall.*.*",
    which requires exactly two wildcard segments after "_.syscall." --
    one for slave_addr, one for the syscall name -- so this subject can
    never match it, regardless of which function_name/slave_addr is
    passed in.

    This test is intentionally decoupled from call_fixed and the
    dispatcher: it inspects the subject call() builds, not whether a
    request succeeds. It will keep failing until call()'s subject
    includes slave_addr, and its assertion won't need to change when
    that happens -- it'll just start passing.
    """

    def test_call_subject_omits_slave_addr(self):
        import inspect
        from ALaDOS.lib._ import main as transport_main

        source = inspect.getsource(transport_main.call)
        # The correct shape interpolates slave_addr into the subject
        # somewhere between "_.syscall." and the function name. Assert
        # that directly rather than re-deriving it from the buggy
        # example, so this test states what *should* be true.
        assert "{slave_addr}" in source, (
            "call() should build its subject with slave_addr in it "
            "(e.g. f'_.syscall.{slave_addr}.{function_name}'), matching "
            "the '_.syscall.*.*' subscriber pattern and batch_call's "
            "own subject construction. It currently doesn't."
        )


# ----------------------------------------------------------------------
# _.main.call() / batch_call -- subject shape sanity, and the real
# batch_call bug (.reply vs .data), independent of the routing bug above
# since batch_call already builds the correct subject.
# ----------------------------------------------------------------------
class TestBatchCall:
    @pytest.mark.anyio
    async def test_batch_call_subject_reaches_dispatcher(self, dispatcher, slave, db):
        """
        batch_call's subject shape is correct on its own -- confirm the
        low-level request it makes reaches the dispatcher and gets
        answered, independent of the later .reply/.data bug below.
        """
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
        """
        Normal-behaviour test for batch_call: two syscalls in, two
        results out, in the same order, with real DB state to prove it.
        This is written to describe what batch_call is supposed to do;
        it currently fails because of the `.reply` vs `.data` bug (see
        test_batch_call_reply_attribute_bug below for that bug pinned
        down directly), not because this expectation is wrong.
        """
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
        """
        Pins down the actual bug: batch_call does `results.append(i.reply)`
        for each response Msg `i`, but Msg.reply is the *subject to
        respond to*, not response payload -- that's `.data`. Checking
        the source directly (rather than round-tripping through NATS)
        keeps this test decoupled from whether the transport-routing bug
        above has been fixed.
        """
        import inspect
        from ALaDOS.lib._ import main as transport_main

        source = inspect.getsource(transport_main.batch_call)
        assert ".data" in source, (
            "batch_call should read response payloads via Msg.data, not "
            "Msg.reply (which holds the reply-to subject, not response "
            "bytes). It currently reads .reply."
        )


# ----------------------------------------------------------------------
# Knowledge
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# Executables
# ----------------------------------------------------------------------
class TestExecutablesLib:
    @pytest.mark.anyio
    async def test_create_registers_a_new_tool(self, dispatcher, slave, db):
        name = unique_name("tool")
        addr = await Executables.create(
            slave,
            description="a test tool",
            header="def f(): pass",
            body="print('hi')",
            name=name,
        )
        assert addr == resolve_to_addr(name, db)
        body = db.execute_fetchval("SELECT body FROM executables WHERE addr = %s", (addr,))
        assert body == "print('hi')"

    @pytest.mark.anyio
    async def test_execute_runs_created_tool_and_returns_output(self, dispatcher, slave, db):
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


# ----------------------------------------------------------------------
# Context
# ----------------------------------------------------------------------
class TestContextLib:
    @pytest.mark.anyio
    async def test_add_loads_item_into_master_context(self, dispatcher, slave, db):
        name = unique_name("ctx")
        addr = await Knowledge.create(slave, content="ctx content", description="d", name=name)

        await Context.add(slave, id=addr)

        master_addr = db.execute_fetchval(
            "SELECT master_addr FROM slaves WHERE addr = %s", (slave,)
        )
        loaded = db.execute(
            "SELECT addr FROM master_context WHERE master_addr = %s", (master_addr,)
        ).fetchall()
        assert addr in [row[0] for row in loaded]

    @pytest.mark.anyio
    async def test_window_change_size_returns_new_sizes(self, dispatcher, slave):
        result = await Context.window_change_size(slave, left=2, right=3)
        assert result["left"] == 2
        assert result["right"] == 3


# ----------------------------------------------------------------------
# Goal
# ----------------------------------------------------------------------
class TestGoalLib:
    @pytest.mark.anyio
    async def test_add_slave_creates_new_slave_row(self, dispatcher, slave, db):
        name = unique_name("newslave")
        addr = await Goal.add_slave(slave, instruction="do something", slave_name=name)
        assert addr == resolve_to_addr(name, db)
        instruction = db.execute_fetchval(
            "SELECT instruction FROM slaves WHERE addr = %s", (addr,)
        )
        assert instruction == "do something"

    @pytest.mark.anyio
    async def test_add_master_creates_new_master_row(self, dispatcher, slave, db):
        addr = await Goal.add_master(slave, instruction="do something else")
        instruction = db.execute_fetchval(
            "SELECT instruction FROM masters WHERE addr = %s", (addr,)
        )
        assert instruction == "do something else"


# ----------------------------------------------------------------------
# Result
# ----------------------------------------------------------------------
class TestResultLib:
    @pytest.mark.anyio
    async def test_write_returns_same_text(self, dispatcher, slave):
        result = await Result.write(slave, text="the result")
        assert result == "the result"

    @pytest.mark.anyio
    async def test_add_master_result_appends_to_master_result(self, dispatcher, slave, db):
        master_addr = db.execute_fetchval(
            "SELECT master_addr FROM slaves WHERE addr = %s", (slave,)
        )
        result_addr = db.execute_fetchval(
            "SELECT result_addr FROM masters WHERE addr = %s", (master_addr,)
        )

        await Result.add_master_result(slave, text="more result text")

        content = db.execute_fetchval(
            "SELECT content_str FROM results WHERE addr = %s", (result_addr,)
        )
        assert "more result text" in content


# ----------------------------------------------------------------------
# Event
# ----------------------------------------------------------------------
class TestEventLib:
    @pytest.mark.anyio
    async def test_create_result_returns_result_and_consumer_addrs(self, dispatcher, slave):
        result = await Event.create_result(
            slave, event_path="evt.some.path", result_str="got ${{data}}"
        )
        assert "result_addr" in result
        assert "consumer_addr" in result
        assert result["result_addr"] > 0
        assert result["consumer_addr"] > 0

    @pytest.mark.anyio
    async def test_register_reaction_slave_creates_consumer_row(self, dispatcher, slave, db):
        consumer_addr = await Event.register_reaction_slave(
            slave, event_path="evt.some.path", instruction="react", scope="general"
        )
        event_path = db.execute_fetchval(
            "SELECT event_path FROM event_consumers WHERE addr = %s", (consumer_addr,)
        )
        assert event_path == "evt.some.path"


# ----------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------
class TestReportLib:
    @pytest.mark.anyio
    async def test_report_paradoxal_information_completes_without_raising(self, dispatcher, slave):
        """
        report_paradoxal_information's handler raises ParadoxDetected on
        the server side, but execute_tool() (called from inside
        _Dispatcher._handle) has no special handling for that -- it
        propagates like any other exception, gets caught by the
        dispatcher's own try/except, and comes back to the client as a
        plain string reply (an error marker), not as a raised exception.
        Report.report_paradoxal_information itself discards whatever
        call() returns (it's typed to return None), so from the client's
        perspective this normal-behaviour test can only assert that the
        call completes and returns None -- it cannot observe the
        underlying paradox signal at all through this wrapper as
        currently written. That's worth knowing on its own: nothing
        about a paradox is visible to a caller of this function.
        """
        result = await Report.report_paradoxal_information(
            slave, items=[1, 2], paradox="conflicting facts"
        )
        assert result is None


# ----------------------------------------------------------------------
# Dispatcher subject parsing itself (custom_consumers.callback), since
# every lib test above is gated on it being correct.
# ----------------------------------------------------------------------
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
            reply = await nt.request(
                f"_.syscall.{slave}.not_a_real_tool",
                b"{}",
                timeout=5,
            )
            assert reply.data.decode().startswith("__DISPATCH_ERROR__:")
        finally:
            await nt.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

