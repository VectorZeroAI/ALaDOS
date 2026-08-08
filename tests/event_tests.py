#!/usr/bin/env python3
"""
Tests for the events subsystem (python/events/*).

Requires a PostgreSQL test database with the ALaDOS schema applied
(same conventions as context_test.py).

Covers:
    - functions.py: create_result_via_event, register_reaction_rmt,
      register_reaction_execute_slave (pure DB read/write logic)
    - event_consumers.py: load_event_consumers's query + build_consumer_data,
      the three consumer_inner functions (call_rmt/execute_slave/fill_result),
      and consumer_outer's per-message dispatch
    - event_gens.py: build_event's path -> NATS-subject conversion
    - types.py: Event.send delegates to the NATS client correctly

NATS and inotify are never touched for real -- they're mocked at the
boundary (connect_nats, the nats Client, Inotify) so these tests run
without any external services beyond postgres.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def anyio_backend():
    """anyio's pytest plugin (already installed) needs this to run async tests."""
    return "asyncio"

from python.utils.conn_factory import Conn, register_all_the_composite_types
from python.events.types import Event, ConsumerCallRmt, ConsumerExecuteSlave, ConsumerFillResult
from python.events.event_consumers import (
    build_consumer_data,
    load_event_consumers,
    call_rmt,
    execute_slave,
    fill_result,
    consumer_outer,
)
from python.events.event_gens import build_event
from python.events.functions import (
    create_result_via_event,
    register_reaction_rmt,
    register_reaction_execute_slave,
)

# ----------------------------------------------------------------------
# Test connection helpers (mirrors context_test.py)
# ----------------------------------------------------------------------
TEST_DSN = dict(
    host="127.0.0.1",
    port=5432,
    dbname=os.environ.get("TEST_DB", "alados_test"),
    user=os.environ.get("TEST_DB_USER", "u0_a453"),
)


def get_test_conn() -> Conn:
    conn = Conn.connect(**TEST_DSN)
    conn.autocommit = True
    conn = register_all_the_composite_types(conn)
    return conn


@pytest.fixture
def db():
    """Test connection inside an explicit transaction that rolls back after test."""
    conn = get_test_conn()
    conn.execute("BEGIN")
    yield conn
    conn.execute("ROLLBACK")
    conn.close()


# ----------------------------------------------------------------------
# Seed helpers
# ----------------------------------------------------------------------
def insert_result(db: Conn, content_str: str = None, ready: bool = False) -> int:
    addr = db.execute_fetchval("SELECT new_addr()")
    if ready:
        db.execute(
            "INSERT INTO results (addr, content_str, ready) VALUES (%s, %s, TRUE)",
            (addr, content_str),
        )
    else:
        db.execute("INSERT INTO results (addr) VALUES (%s)", (addr,))
    return addr


def insert_rmt(db: Conn) -> int:
    """A reusable_master_template row (rmt_addr FK target for event_call_rmt)."""
    return db.execute_fetchval(
        "INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr"
    )


# ----------------------------------------------------------------------
# events/functions.py
# ----------------------------------------------------------------------
class TestCreateResultViaEvent:
    def test_creates_result_and_consumer(self, db):
        addr = create_result_via_event("foo.bar.baz", "hello ${{data}}", db)

        action_type = db.execute_fetchval(
            "SELECT action_type FROM event_consumers WHERE addr = %s", (addr,)
        )
        event_path = db.execute_fetchval(
            "SELECT event_path FROM event_consumers WHERE addr = %s", (addr,)
        )
        assert action_type == "fill_result"
        assert event_path == "foo.bar.baz"

    def test_links_event_call_fill_result_row(self, db):
        addr = create_result_via_event("foo.bar", "hello", db)

        result_addr, result_str = db.execute(
            "SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s",
            (addr,),
        ).fetchone()

        assert result_str == "hello"
        # the linked result row exists, not yet ready
        ready = db.execute_fetchval(
            "SELECT ready FROM results WHERE addr = %s", (result_addr,)
        )
        assert ready is False

    def test_returns_consumer_addr_not_result_addr(self, db):
        consumer_addr = create_result_via_event("path", "str", db)
        result_addr = db.execute_fetchval(
            "SELECT result_addr FROM event_call_fill_result WHERE addr = %s",
            (consumer_addr,),
        )
        assert consumer_addr != result_addr


class TestRegisterReactionRmt:
    def test_creates_consumer_and_call_rmt_row(self, db):
        rmt_addr = insert_rmt(db)
        args = {"color": "GREEN"}

        consumer_addr = register_reaction_rmt("some.event.path", rmt_addr, args, db)

        action_type, event_path = db.execute(
            "SELECT action_type, event_path FROM event_consumers WHERE addr = %s",
            (consumer_addr,),
        ).fetchone()
        assert action_type == "call_rmt"
        assert event_path == "some.event.path"

        stored_rmt_addr, stored_args = db.execute(
            "SELECT rmt_addr, args FROM event_call_rmt WHERE addr = %s",
            (consumer_addr,),
        ).fetchone()
        assert stored_rmt_addr == rmt_addr
        assert stored_args == args

    def test_rejects_nonexistent_rmt(self, db):
        with pytest.raises(Exception):
            register_reaction_rmt("path", 999999999, {}, db)


class TestRegisterReactionExecuteSlave:
    def test_creates_consumer_and_execute_slave_row(self, db):
        consumer_addr = register_reaction_execute_slave(
            "some.path", "do the thing with ${{data}}", "general", db
        )

        action_type = db.execute_fetchval(
            "SELECT action_type FROM event_consumers WHERE addr = %s", (consumer_addr,)
        )
        assert action_type == "execute_slave"

        instruction, scope = db.execute(
            "SELECT instruction, scope FROM event_call_execute_slave WHERE addr = %s",
            (consumer_addr,),
        ).fetchone()
        assert instruction == "do the thing with ${{data}}"
        assert scope == "general"


# ----------------------------------------------------------------------
# events/event_consumers.py -- build_consumer_data
# ----------------------------------------------------------------------
class TestBuildConsumerData:
    def test_call_rmt_row(self):
        row = ("path.a", "call_rmt", 42, {"x": "y"})
        consumer = build_consumer_data(row)
        assert isinstance(consumer, ConsumerCallRmt)
        assert consumer.event_path == "path.a"
        assert consumer.rmt_id == 42
        assert consumer.args == {"x": "y"}

    def test_execute_slave_row(self):
        row = ("path.b", "execute_slave", "do stuff", "general")
        consumer = build_consumer_data(row)
        assert isinstance(consumer, ConsumerExecuteSlave)
        assert consumer.instruction == "do stuff"
        assert consumer.scope == "general"

    def test_fill_result_row(self):
        row = ("path.c", "fill_result", 7, "the result string")
        consumer = build_consumer_data(row)
        assert isinstance(consumer, ConsumerFillResult)
        assert consumer.result_addr == 7
        assert consumer.result_str == "the result string"

    def test_unknown_action_type_raises(self):
        row = ("path.d", "not_a_real_action", None, None)
        with pytest.raises(ValueError):
            build_consumer_data(row)


# ----------------------------------------------------------------------
# events/event_consumers.py -- load_event_consumers query shape
# ----------------------------------------------------------------------
class TestLoadEventConsumersQuery:
    """
    Exercises the actual SQL join in load_event_consumers against a real DB,
    with connect_nats mocked out so no live NATS server is required.

    load_event_consumers now builds each ConsumerData row and wraps it
    directly via consumer_outer(consumer, nt) -- it no longer goes through a
    separate create_consumer step, and consumer_outer's own dispatch (the
    match on consumer_data) only runs once messages actually arrive on the
    NATS subscription, not at load time. So load_event_consumers itself
    should just succeed and return one coroutine per consumer row, regardless
    of action_type -- there's nothing here to dispatch yet.

    Each returned item is an unawaited/unscheduled coroutine (consumer_outer
    was called but never awaited), so we close() them after counting to
    avoid "coroutine was never awaited" warnings -- this suite isn't
    responsible for verifying the coroutines actually run correctly here;
    that's covered separately in TestConsumerOuterDispatch below.
    """

    def _fake_loop(self):
        """
        A stand-in event loop whose run_until_complete actually drives the
        passed-in coroutine to completion (closing it if unused), instead of
        leaving connect_nats()'s coroutine dangling.
        """
        loop = MagicMock()

        def _run_until_complete(coro):
            try:
                coro.send(None)
            except StopIteration as e:
                return e.value
            else:
                coro.close()
                return MagicMock()

        loop.run_until_complete = _run_until_complete
        return loop

    @staticmethod
    def _close_all(consumers):
        for c in consumers:
            c.close()

    def test_no_consumers_returns_empty_list(self, db):
        with patch("python.events.event_consumers.connect_nats", new=AsyncMock()):
            consumers = load_event_consumers(db, self._fake_loop())

        assert consumers == []

    def test_loads_fill_result_consumer(self, db):
        create_result_via_event("evt.one", "payload text", db)

        with patch("python.events.event_consumers.connect_nats", new=AsyncMock()):
            consumers = load_event_consumers(db, self._fake_loop())

        assert len(consumers) == 1
        self._close_all(consumers)

    def test_loads_call_rmt_consumer(self, db):
        rmt_addr = insert_rmt(db)
        register_reaction_rmt("evt.two", rmt_addr, {"a": "b"}, db)

        with patch("python.events.event_consumers.connect_nats", new=AsyncMock()):
            consumers = load_event_consumers(db, self._fake_loop())

        assert len(consumers) == 1
        self._close_all(consumers)

    def test_loads_execute_slave_consumer(self, db):
        register_reaction_execute_slave("evt.three", "instr", "general", db)

        with patch("python.events.event_consumers.connect_nats", new=AsyncMock()):
            consumers = load_event_consumers(db, self._fake_loop())

        assert len(consumers) == 1
        self._close_all(consumers)

    def test_loads_multiple_mixed_consumers(self, db):
        rmt_addr = insert_rmt(db)
        create_result_via_event("evt.a", "x", db)
        register_reaction_rmt("evt.b", rmt_addr, {}, db)
        register_reaction_execute_slave("evt.c", "instr", "general", db)

        with patch("python.events.event_consumers.connect_nats", new=AsyncMock()):
            consumers = load_event_consumers(db, self._fake_loop())

        assert len(consumers) == 3
        self._close_all(consumers)


# ----------------------------------------------------------------------
# events/event_consumers.py -- consumer_inner functions
# ----------------------------------------------------------------------
class FakeEvent:
    """Stand-in for events.types.Event, avoiding a real NATS client dependency."""
    def __init__(self, event_path: str, payload: str):
        self.event_path = event_path
        self.payload = payload


class TestCallRmt:
    def test_calls_activate_as_master_with_merged_args(self, db):
        consumer_data = ConsumerCallRmt(
            event_path="evt.rmt",
            action_type="call_rmt",
            rmt_id=1,
            args={"color": "GREEN"},
        )
        event = FakeEvent("evt.rmt", "the payload")

        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"), \
             patch("python.events.event_consumers.activate_as_master") as mock_activate:
            call_rmt(event, consumer_data)

        mock_activate.assert_called_once()
        _, kwargs = mock_activate.call_args
        assert kwargs["inputs"]["data"] == "the payload"
        assert kwargs["inputs"]["subject"] == "evt.rmt"
        assert kwargs["inputs"]["color"] == "GREEN"


class TestExecuteSlave:
    def test_substitutes_data_and_subject_into_instruction(self, db):
        consumer_data = ConsumerExecuteSlave(
            event_path="evt.slave",
            action_type="execute_slave",
            instruction="React to ${{data}} from ${{subject}}",
            scope="general",
        )
        event = FakeEvent("evt.slave", "payload!")

        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"):
            execute_slave(event, consumer_data)

        slave_instruction = db.execute_fetchval(
            "SELECT instruction FROM slaves ORDER BY addr DESC LIMIT 1"
        )
        assert slave_instruction == "React to payload! from evt.slave"


class TestFillResult:
    def test_fills_the_linked_result(self, db):
        result_addr = insert_result(db)
        consumer_data = ConsumerFillResult(
            event_path="evt.fill",
            action_type="fill_result",
            result_addr=result_addr,
            result_str="Got ${{data}} on ${{event}}",
        )
        event = FakeEvent("evt.fill", "42")

        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"):
            fill_result(event, consumer_data)

        content_str, ready = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = %s", (result_addr,)
        ).fetchone()
        assert content_str == "Got 42 on evt.fill"
        assert ready is True

    def test_end_to_end_create_result_via_event_then_fill(self, db):
        """create_result_via_event + fill_result should compose correctly."""
        consumer_addr = create_result_via_event("evt.pipeline", "answer=${{data}}", db)

        result_addr, result_str = db.execute(
            "SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s",
            (consumer_addr,),
        ).fetchone()
        consumer_data = ConsumerFillResult(
            event_path="evt.pipeline",
            action_type="fill_result",
            result_addr=result_addr,
            result_str=result_str,
        )
        event = FakeEvent("evt.pipeline", "17")

        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"):
            fill_result(event, consumer_data)

        content_str, ready = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = %s", (result_addr,)
        ).fetchone()
        assert content_str == "answer=17"
        assert ready is True


# ----------------------------------------------------------------------
# events/event_consumers.py -- consumer_outer dispatch
# ----------------------------------------------------------------------
class FakeNatsMsg:
    """Stand-in for a nats.aio.msg.Msg as consumed by consumer_outer."""
    def __init__(self, subject: str, data: bytes):
        self.subject = subject
        self.data = data


class FakeSubscription:
    """Stand-in for what nt.subscribe(...) returns -- only .messages is used."""
    def __init__(self, msgs: list[FakeNatsMsg]):
        self._msgs = msgs

    @property
    async def messages(self):
        for m in self._msgs:
            yield m


def _make_fake_nats_client(msgs: list[FakeNatsMsg]) -> AsyncMock:
    nt = AsyncMock()
    nt.subscribe = AsyncMock(return_value=FakeSubscription(msgs))
    return nt


class TestConsumerOuterDispatch:
    """
    consumer_outer subscribes on NATS and, for each message, dispatches to the
    matching consumer_inner (call_rmt / execute_slave / fill_result) via
    loop.run_in_executor. These tests fake out the NATS subscription with one
    message each, and patch run_in_executor to capture the (func, event,
    consumer_data) it was called with -- without actually running the real
    DB-touching consumer_inner or needing a real event loop's executor.
    """

    @pytest.mark.anyio
    async def test_dispatches_call_rmt_to_call_rmt_inner(self):
        consumer_data = ConsumerCallRmt("evt.rmt", "call_rmt", 1, {"a": "b"})
        nt = _make_fake_nats_client([FakeNatsMsg("evt.rmt", b"payload")])

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            await consumer_outer(consumer_data, nt)

        mock_loop.run_in_executor.assert_called_once()
        args = mock_loop.run_in_executor.call_args[0]
        # (executor, func, event, consumer_data)
        assert args[1] is call_rmt
        assert args[3] is consumer_data

    @pytest.mark.anyio
    async def test_dispatches_execute_slave_to_execute_slave_inner(self):
        consumer_data = ConsumerExecuteSlave("evt.slave", "execute_slave", "i", "general")
        nt = _make_fake_nats_client([FakeNatsMsg("evt.slave", b"payload")])

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            await consumer_outer(consumer_data, nt)

        mock_loop.run_in_executor.assert_called_once()
        args = mock_loop.run_in_executor.call_args[0]
        assert args[1] is execute_slave
        assert args[3] is consumer_data

    @pytest.mark.anyio
    async def test_dispatches_fill_result_to_fill_result_inner(self):
        consumer_data = ConsumerFillResult("evt.fill", "fill_result", 1, "s")
        nt = _make_fake_nats_client([FakeNatsMsg("evt.fill", b"payload")])

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            await consumer_outer(consumer_data, nt)

        mock_loop.run_in_executor.assert_called_once()
        args = mock_loop.run_in_executor.call_args[0]
        assert args[1] is fill_result
        assert args[3] is consumer_data

    @pytest.mark.anyio
    async def test_dispatches_once_per_message(self):
        consumer_data = ConsumerFillResult("evt.fill", "fill_result", 1, "s")
        nt = _make_fake_nats_client([
            FakeNatsMsg("evt.fill", b"one"),
            FakeNatsMsg("evt.fill", b"two"),
            FakeNatsMsg("evt.fill", b"three"),
        ])

        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_get_loop.return_value = mock_loop
            await consumer_outer(consumer_data, nt)

        assert mock_loop.run_in_executor.call_count == 3

    @pytest.mark.anyio
    async def test_subscribes_to_the_consumer_data_event_path(self):
        consumer_data = ConsumerFillResult("evt.some.specific.path", "fill_result", 1, "s")
        nt = _make_fake_nats_client([])

        with patch("asyncio.get_running_loop"):
            await consumer_outer(consumer_data, nt)

        nt.subscribe.assert_awaited_once_with("evt.some.specific.path")


# ----------------------------------------------------------------------
# events/event_gens.py -- build_event
# ----------------------------------------------------------------------
class TestBuildEvent:
    """
    build_event now requires an `nt` (NATS client) kwarg, passed through to the
    constructed Event. Using a MagicMock here since these tests only check
    event_path/payload construction, not actual publishing.
    """

    def test_joins_parts_with_dots(self):
        event = build_event("a", "b", "c", payload="p", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == "a.b.c"
        assert event.payload == "p"

    def test_lowercases_the_path(self):
        event = build_event("A", "B", payload="p", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == "a.b"

    def test_applies_converter_to_each_part(self):
        event = build_event(
            "/foo/bar", "CREATE",
            payload="",
            converter=lambda s: s.replace("/", ".").removeprefix("."),
            nt=MagicMock(),
        )
        # "/foo/bar" -> ".foo.bar" -> stripped leading "." -> "foo.bar"
        assert event.event_path == "foo.bar.create"

    def test_no_parts_gives_empty_path(self):
        event = build_event(payload="x", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == ""


# ----------------------------------------------------------------------
# events/types.py -- Event.send
# ----------------------------------------------------------------------
class TestEventSend:
    """Event.__init__ is a plain (sync) method; construction itself needs no await."""

    def test_init_sets_attributes(self):
        mock_client = AsyncMock()
        event = Event("some.path", "hello", mock_client)
        assert event.event_path == "some.path"
        assert event.payload == "hello"

    @pytest.mark.anyio
    async def test_send_publishes_encoded_payload_to_client(self):
        mock_client = AsyncMock()
        event = Event("some.path", "hello", mock_client)

        await event.send()

        mock_client.publish.assert_awaited_once_with("some.path", b"hello")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

