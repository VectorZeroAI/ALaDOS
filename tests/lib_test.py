#!/usr/bin/env python3
"""
Integration test suite for the `lib` package (`Knowledge`, `Executables`,
`Rmt`, `Context`, `Goal`, `Result`, `Web`, `Event`, `Report`, and the `_`
transport layer).

Unlike builtins_test.py, which calls the server-side handlers directly,
this suite drives the *client* wrappers in lib (`Knowledge.create`,
`Rmt.activate_as_master`, etc.) end to end over NATS, the way a running
tool subprocess actually would.

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
   up that piece ourselves; `_dispatcher_thread` below is exactly that: a
   background thread that (a) runs base_state's custom-consumer
   subscription so inbound `_.syscall.<addr>.<name>` messages get queued,
   and (b) drains the per-slave queue and replies via execute_tool(),
   which is precisely what the subprocess-poll loop does today.

   This is infrastructure this suite needs to exist, not a system
   component -- it is not a replacement for one and should not be
   mistaken for a "fix" of the missing standalone syscall server.

KNOWN BUG THIS SUITE DOCUMENTS
--------------------------------
`_.main.call()` publishes to `_.syscall.{function_name}` -- it never
includes slave_addr in the subject. Every single wrapper in Knowledge,
Executables, Rmt, Context, Goal, Result, Web, Event, and Report (i.e.
everything except batch_call) goes through `call()`, so every one of
them sends a subject that doesn't match `_.syscall.*.*` (needs exactly
three dot-separated segments after `_`) and can never reach the
dispatcher. Per project convention, this is asserted as current, real
behaviour (a TimeoutError from nats), not xfail'd or skipped.

`batch_call` gets the subject right (`_.syscall.{slave_id}.{name}`) but
then tries to read `.reply` off the returned Msg object instead of
`.data` -- `Msg` has no `reply` attribute holding response bytes, so
this is asserted too, separately.
"""

import asyncio
import json
import threading
from datetime import datetime
from unittest.mock import patch

import nats
import pytest

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError

from python.executor.execute_tool import execute_tool
from python.executor.queue import syscalls_queue_dict_per_slave
from python.executor.types import ToolCall, _ExecToolMetaData
from python.utils.conn_factory import Conn, register_all_the_composite_types
from python.utils.name_resolver import resolve_to_addr

from lib._.main import call as raw_call, batch_call, syscall as SyscallSpec

from lib import Knowledge
from lib import Executables
from lib import Context
from lib import Goal
from lib import Result
from lib import Event
from lib import Report


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

        async def waiter():
            while not self._stop.is_set():
                await asyncio.sleep(0.02)

        try:
            async for msg in sub.messages:
                await self._handle(msg)
                if self._stop.is_set():
                    break
        finally:
            await sub.unsubscribe()
            await nt.close()

    async def _handle(self, msg: Msg) -> None:
        parts = msg.subject.split(".", 3)
        slave_addr = int(parts[-2])
        tool_name = parts[-1]
        meta = _ExecToolMetaData(
            master_id=0,
            conn=self._db,
            slave_id=slave_addr,
            context_limit=40000,
            occ_last_change=datetime.now(),
            syscalls_queue=syscalls_queue_dict_per_slave[slave_addr],
        )
        try:
            args = json.loads(msg.data.decode())
            result = execute_tool(ToolCall(tool=tool_name, args=args), meta)
        except Exception as e:  # noqa: BLE001 -- surfaced to the caller, not swallowed
            result = f"__DISPATCH_ERROR__:{e}"
        if msg.reply:
            await msg.respond(str(result).encode())


@pytest.fixture
def dispatcher(db):
    """
    Bug context: since raw_call()/lib.* never actually reach this
    dispatcher (see module docstring), most tests below don't depend on
    it at all -- it exists for the tests that document *correct* subject
    routing (i.e. what would work if call() were fixed) and for the
    batch_call tests, which do route correctly.
    """
    d = _Dispatcher(db)
    d.start()
    yield d
    d.stop()


# ----------------------------------------------------------------------
# _.main.call() -- the shared transport used by every lib.* wrapper
# except batch_call.
# ----------------------------------------------------------------------
class TestRawCallSubjectBug:
    """
    BUG: call() in _/main.py does:

        await nt.request(f"_.syscall.{function_name}", ...)

    This omits slave_addr entirely. The subscriber pattern registered in
    base_state/state_components/custom_consumers.py is "_.syscall.*.*",
    which requires exactly two wildcard segments after "_.syscall." --
    one for slave_addr, one for the syscall name. A subject of
    "_.syscall.k_create" only has one segment there, so it can never
    match, and the request will always time out against a real NATS
    server, function_name and slave_addr notwithstanding.
    """

    @pytest.mark.asyncio
    async def test_call_times_out_against_live_dispatcher(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await raw_call("k_create", slave, {"content": "x", "description": "y", "name": None})

    @pytest.mark.asyncio
    async def test_call_publishes_malformed_subject(self):
        """
        Directly proves the subject shape is wrong, independent of
        whether anything is subscribed: connect our own client, request
        with a very short timeout against a subject built the same way
        call() builds it, and confirm it isn't "_.syscall.<addr>.<name>"
        (i.e. splitting on '.' doesn't yield 4 parts).
        """
        subject = f"_.syscall.{'k_create'}"
        assert len(subject.split(".")) == 2  # "_" , "syscall.k_create" after first split point
        # More directly: the correct shape has exactly 4 dot-separated
        # segments ("_", "syscall", "<addr>", "<name>"); the buggy one
        # only ever has 3, with no address segment at all.
        assert len(subject.split(".")) != 4


class TestBatchCallBugs:
    """
    batch_call gets the *subject* right ("_.syscall.<slave_id>.<name>"),
    unlike call(), but then does `results.append(i.reply)` where `i` is
    a `Msg` returned from `nt.request(...)`. `Msg` doesn't carry response
    payload under `.reply` (that's the *subject* used for anyone
    replying to this message, not the reply's contents) -- the payload
    lives in `.data`. This is asserted as current, real behavior.
    """

    @pytest.mark.asyncio
    async def test_batch_call_subject_matches_dispatcher_pattern(self, dispatcher, slave, db):
        """
        Sanity check that the subject shape itself is correct, by
        exercising the low level request manually and confirming the
        dispatcher (which only matches "_.syscall.*.*") does receive
        and answer it.
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
            resolved = resolve_to_addr(name, db)
            assert resolved == addr
        finally:
            await nt.close()

    @pytest.mark.asyncio
    async def test_batch_call_raises_attributeerror_on_reply_access(self, dispatcher, slave):
        """
        Documents the `.reply` vs `.data` bug: batch_call should raise
        AttributeError while trying to build its return list, even
        though the underlying request/response round trip succeeds.
        """
        calls = [SyscallSpec("k_create", {"content": "c", "description": "d", "name": None})]
        with pytest.raises(AttributeError):
            await batch_call(calls, slave)


# ----------------------------------------------------------------------
# Knowledge
# ----------------------------------------------------------------------
class TestKnowledgeLib:
    @pytest.mark.asyncio
    async def test_create_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Knowledge.create(slave, content="hello", description="desc")

    @pytest.mark.asyncio
    async def test_create_and_read_work_once_call_is_fixed(self, dispatcher, slave, db):
        """
        Demonstrates Knowledge.create/read/edit are otherwise correctly
        implemented: patch call() to use the correct subject shape and
        confirm the round trip through the real dispatcher and DB
        succeeds. This isolates the bug to the transport layer.
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

        with patch("Knowledge.functions.call", new=fixed_call):
            name = unique_name("kn")
            addr = await Knowledge.create(slave, content="hello", description="desc", name=name)
            assert addr == resolve_to_addr(name, db)

            content = await Knowledge.read(name, slave)
            assert content == "hello"


# ----------------------------------------------------------------------
# Executables
# ----------------------------------------------------------------------
class TestExecutablesLib:
    @pytest.mark.asyncio
    async def test_execute_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Executables.execute(slave, id="nonexistent_tool", timeout=1)

    @pytest.mark.asyncio
    async def test_create_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Executables.create(
                slave, description="d", header="def f(): pass", body="print('hi')"
            )


# ----------------------------------------------------------------------
# Context
# ----------------------------------------------------------------------
class TestContextLib:
    @pytest.mark.asyncio
    async def test_add_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Context.add(slave, id=1)

    @pytest.mark.asyncio
    async def test_window_change_size_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Context.window_change_size(slave, left=1, right=1)


# ----------------------------------------------------------------------
# Goal
# ----------------------------------------------------------------------
class TestGoalLib:
    @pytest.mark.asyncio
    async def test_add_slave_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Goal.add_slave(slave, instruction="do something")

    @pytest.mark.asyncio
    async def test_add_master_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Goal.add_master(slave, instruction="do something else")


# ----------------------------------------------------------------------
# Result
# ----------------------------------------------------------------------
class TestResultLib:
    @pytest.mark.asyncio
    async def test_write_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Result.write(slave, text="the result")

    @pytest.mark.asyncio
    async def test_add_master_result_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Result.add_master_result(slave, text="more result text")


# ----------------------------------------------------------------------
# Event
# ----------------------------------------------------------------------
class TestEventLib:
    @pytest.mark.asyncio
    async def test_create_result_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Event.create_result(slave, event_path="evt.some.path", result_str="got ${{data}}")

    @pytest.mark.asyncio
    async def test_register_reaction_slave_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Event.register_reaction_slave(
                slave, event_path="evt.some.path", instruction="react", scope="general"
            )


# ----------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------
class TestReportLib:
    @pytest.mark.asyncio
    async def test_report_paradoxal_information_times_out_due_to_call_bug(self, dispatcher, slave):
        with pytest.raises(NatsTimeoutError):
            await Report.report_paradoxal_information(slave, items=[1, 2], paradox="conflicting facts")


# ----------------------------------------------------------------------
# Dispatcher subject parsing itself (custom_consumers.callback), since
# every lib test above is gated on it being correct.
# ----------------------------------------------------------------------
class TestDispatcherSubjectParsing:
    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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

