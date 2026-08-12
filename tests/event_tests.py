#!/usr/bin/env python3
"""
Tests for the events subsystem.
"""
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
pytestmark = pytest.mark.anyio

from .conftest import db  # noqa: F401

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


def insert_result(db, content_str=None, ready=False):
    addr = db.execute_fetchval("SELECT new_addr()")
    if ready:
        db.execute("INSERT INTO results (addr, content_str, ready) VALUES (%s, %s, TRUE)", (addr, content_str))
    else:
        db.execute("INSERT INTO results (addr) VALUES (%s)", (addr,))
    return addr


def insert_rmt(db):
    return db.execute_fetchval("INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr")


class TestCreateResultViaEvent:
    def test_creates_result_and_consumer(self, db):
        result = create_result_via_event("foo.bar.baz", "hello ${{data}}", db)
        action_type = db.execute_fetchval(
            "SELECT action_type FROM event_consumers WHERE addr = %s", (result.consumer_addr,)
        )
        event_path = db.execute_fetchval(
            "SELECT event_path FROM event_consumers WHERE addr = %s", (result.consumer_addr,)
        )
        assert action_type == "fill_result"
        assert event_path == "foo.bar.baz"

    def test_links_event_call_fill_result_row(self, db):
        result = create_result_via_event("foo.bar", "hello", db)
        result_addr, result_str = db.execute(
            "SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s",
            (result.consumer_addr,),
        ).fetchone()
        assert result_str == "hello"
        ready = db.execute_fetchval("SELECT ready FROM results WHERE addr = %s", (result_addr,))
        assert ready is False

    def test_returns_consumer_addr_not_result_addr(self, db):
        result = create_result_via_event("path", "str", db)
        result_addr = db.execute_fetchval(
            "SELECT result_addr FROM event_call_fill_result WHERE addr = %s", (result.consumer_addr,),
        )
        assert result.consumer_addr != result_addr

    def test_end_to_end_create_result_via_event_then_fill(self, db):
        ret = create_result_via_event("evt.pipeline", "answer=${{data}}", db)
        result_addr, result_str = db.execute(
            "SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s", (ret.consumer_addr,),
        ).fetchone()
        assert result_addr == ret.result_addr
        consumer_data = ConsumerFillResult(
            event_path="evt.pipeline",
            action_type="fill_result",
            result_addr=result_addr,
            result_str=result_str,
        )
        event = FakeEvent("evt.pipeline", "17")
        with patch("python.events.event_consumers.conn_factory", return_value=db), patch.object(db, "close"):
            fill_result(event, consumer_data)
        content_str, ready = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = %s", (result_addr,)
        ).fetchone()
        assert content_str == "answer=17"
        assert ready is True


class TestRegisterReactionRmt:
    def test_creates_consumer_and_call_rmt_row(self, db):
        rmt_addr = insert_rmt(db)
        args = {"color": "GREEN"}
        consumer_addr = register_reaction_rmt("some.event.path", rmt_addr, args, db)
        action_type, event_path = db.execute(
            "SELECT action_type, event_path FROM event_consumers WHERE addr = %s", (consumer_addr,),
        ).fetchone()
        assert action_type == "call_rmt"
        assert event_path == "some.event.path"
        stored_rmt_addr, stored_args = db.execute(
            "SELECT rmt_addr, args FROM event_call_rmt WHERE addr = %s", (consumer_addr,),
        ).fetchone()
        assert stored_rmt_addr == rmt_addr
        assert stored_args == args

    def test_rejects_nonexistent_rmt(self, db):
        with pytest.raises(Exception):
            register_reaction_rmt("path", 999999999, {}, db)


class TestRegisterReactionExecuteSlave:
    def test_creates_consumer_and_execute_slave_row(self, db):
        consumer_addr = register_reaction_execute_slave("some.path", "do the thing with ${{data}}", "general", db)
        action_type = db.execute_fetchval(
            "SELECT action_type FROM event_consumers WHERE addr = %s", (consumer_addr,)
        )
        assert action_type == "execute_slave"
        instruction, scope = db.execute(
            "SELECT instruction, scope FROM event_call_execute_slave WHERE addr = %s", (consumer_addr,),
        ).fetchone()
        assert instruction == "do the thing with ${{data}}"
        assert scope == "general"


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


class TestLoadEventConsumersQuery:
    def _fake_loop(self):
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


class FakeEvent:
    def __init__(self, event_path, payload):
        self.event_path = event_path
        self.payload = payload


class TestCallRmt:
    def test_calls_activate_as_master_with_merged_args(self, db):
        consumer_data = ConsumerCallRmt("evt.rmt", "call_rmt", 1, {"color": "GREEN"})
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
        consumer_data = ConsumerExecuteSlave("evt.slave", "execute_slave",
                                             "React to ${{data}} from ${{subject}}", "general")
        event = FakeEvent("evt.slave", "payload!")
        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"):
            execute_slave(event, consumer_data)
        slave_instruction = db.execute_fetchval("SELECT instruction FROM slaves ORDER BY addr DESC LIMIT 1")
        assert slave_instruction == "React to payload! from evt.slave"


class TestFillResult:
    def test_fills_the_linked_result(self, db):
        result_addr = insert_result(db)
        consumer_data = ConsumerFillResult("evt.fill", "fill_result", result_addr, "Got ${{data}} on ${{event}}")
        event = FakeEvent("evt.fill", "42")
        with patch("python.events.event_consumers.conn_factory", return_value=db), \
             patch.object(db, "close"):
            fill_result(event, consumer_data)
        content_str, ready = db.execute(
            "SELECT content_str, ready FROM results WHERE addr = %s", (result_addr,)
        ).fetchone()
        assert content_str == "Got 42 on evt.fill"
        assert ready is True


class FakeNatsMsg:
    def __init__(self, subject, data):
        self.subject = subject
        self.data = data


class FakeSubscription:
    def __init__(self, msgs):
        self._msgs = msgs

    @property
    async def messages(self):
        for m in self._msgs:
            yield m


def _make_fake_nats_client(msgs):
    nt = AsyncMock()
    nt.subscribe = AsyncMock(return_value=FakeSubscription(msgs))
    return nt


class TestConsumerOuterDispatch:
    @pytest.mark.anyio
    async def test_dispatches_call_rmt_to_call_rmt_inner(self):
        consumer_data = ConsumerCallRmt("evt.rmt", "call_rmt", 1, {"a": "b"})
        nt = _make_fake_nats_client([FakeNatsMsg("evt.rmt", b"payload")])
        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = AsyncMock()
            mock_get_loop.return_value = mock_loop
            await consumer_outer(consumer_data, nt)
        mock_loop.run_in_executor.assert_called_once()
        args = mock_loop.run_in_executor.call_args[0]
        assert args[1] is call_rmt
        assert args[3] is consumer_data

    @pytest.mark.anyio
    async def test_dispatches_execute_slave_to_execute_slave_inner(self):
        consumer_data = ConsumerExecuteSlave("evt.slave", "execute_slave", "i", "general")
        nt = _make_fake_nats_client([FakeNatsMsg("evt.slave", b"payload")])
        with patch("asyncio.get_running_loop") as mock_get_loop:
            mock_loop = AsyncMock()
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
            mock_loop = AsyncMock()
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
            mock_loop = AsyncMock()
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


class TestBuildEvent:
    def test_joins_parts_with_dots(self):
        event = build_event("a", "b", "c", payload="p", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == "a.b.c"
        assert event.payload == "p"

    def test_lowercases_the_path(self):
        event = build_event("A", "B", payload="p", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == "a.b"

    def test_applies_converter_to_each_part(self):
        event = build_event("/foo/bar", "CREATE", payload="",
                            converter=lambda s: s.replace("/", ".").removeprefix("."),
                            nt=MagicMock())
        assert event.event_path == "foo.bar.create"

    def test_no_parts_gives_empty_path(self):
        event = build_event(payload="x", converter=lambda x: x, nt=MagicMock())
        assert event.event_path == ""


class TestEventSend:
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
