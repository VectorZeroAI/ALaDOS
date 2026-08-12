#!/usr/bin/env python3
"""
Tests for the base_state subsystem (python/base_state/*).
"""
from unittest.mock import MagicMock, patch

import pytest

from python.utils.conn_factory import Conn
from python.base_state.types import (
    Cronjob,
    EventConsumers,
    Executable,
    Knowledge,
    Masters,
    Results,
    Rmt,
    Slaves,
    real_new_addr,
    virtual_new_addr,
)
import python.base_state.registry as registry_mod
from python.base_state.registry import (
    ADDR_REGISTER,
    REGISTERERS_REGISTRY,
    SYSTEM_ADDRS_LIST,
    register,
    register_cronjob,
    register_event_consumer,
    register_executable,
    register_knowledge,
    register_master,
    register_result,
    register_rmt,
    register_slaves,
)
import python.base_state.main as base_state_main

from .conftest import db  # noqa: F401


def insert_result(db):
    return db.execute_fetchval("INSERT INTO results (addr) VALUES (new_addr()) RETURNING addr")


def insert_addr(db):
    return db.execute_fetchval("SELECT new_addr()")


def insert_rmt_template(db):
    return db.execute_fetchval("INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr")


@pytest.fixture(autouse=True)
def isolate_registry():
    """Prevent global registry state and un-awaited consumer coroutines leaking."""
    old_system = list(registry_mod.SYSTEM_ADDRS_LIST)
    old_addr_reg = dict(registry_mod.ADDR_REGISTER)

    # Import-time registration creates coroutine objects for custom consumers.
    # These tests do not run the event-consumer service, so keep them out of
    # the test process and explicitly close them instead of letting pytest's
    # unraisable-exception hook report "coroutine was never awaited".
    for consumer in registry_mod.CUSTOM_CONSUMERS:
        close = getattr(consumer, "close", None)
        if close is not None:
            close()
    registry_mod.CUSTOM_CONSUMERS.clear()

    yield

    for consumer in registry_mod.CUSTOM_CONSUMERS:
        close = getattr(consumer, "close", None)
        if close is not None:
            close()
    registry_mod.CUSTOM_CONSUMERS.clear()
    registry_mod.SYSTEM_ADDRS_LIST[:] = old_system
    registry_mod.ADDR_REGISTER.clear()
    registry_mod.ADDR_REGISTER.update(old_addr_reg)


class TestRealNewAddr:
    def test_returns_int(self, db):
        with patch("python.base_state.types.conn_factory_raw", return_value=db):
            addr = real_new_addr()
        assert isinstance(addr, int)

    def test_inserts_real_row_into_addrs(self, db):
        with patch("python.base_state.types.conn_factory_raw", return_value=db):
            addr = real_new_addr()
        found = db.execute_fetchval("SELECT addr FROM addrs WHERE addr = %s", (addr,))
        assert found == addr

    def test_successive_calls_return_distinct_real_addrs(self, db):
        with patch("python.base_state.types.conn_factory_raw", return_value=db):
            a1 = real_new_addr()
            a2 = real_new_addr()
        assert a1 != a2


class TestVirtualNewAddr:
    def test_returns_int(self):
        addr = virtual_new_addr()
        assert isinstance(addr, int)

    def test_dataclass_default_uses_virtual_address(self):
        item = Knowledge("description", "content", "virtual_name")
        assert isinstance(item.addr, int)

    def test_explicit_address_is_preserved(self):
        item = Knowledge("description", "content", "explicit_name", 123456)
        assert item.addr == 123456


class TestItemDataclasses:
    def test_knowledge_positional_construction(self):
        k = Knowledge("a description", "some content", "k_name", 1)
        assert k.description == "a description"
        assert k.content == "some content"
        assert k.name == "k_name"
        assert k.addr == 1

    def test_executable_positional_construction(self):
        e = Executable("desc", "print(1)", "header text", "e_name", 2)
        assert e.body == "print(1)"
        assert e.header == "header text"
        assert e.addr == 2

    def test_results_positional_construction(self):
        r = Results("content", {"k": "v"}, "r_name", 3, True)
        assert r.content_str == "content"
        assert r.metadata == {"k": "v"}
        assert r.ready is True

    def test_results_ready_defaults_false(self):
        r = Results("content", {"k": "v"}, "r_name", 3)
        assert r.ready is False

    def test_event_consumers_positional_construction(self):
        ec = EventConsumers("evt.path", "call_rmt", 1, {"a": "b"}, 4)
        assert ec.event_path == "evt.path"
        assert ec.action_type == "call_rmt"
        assert ec.field1 == 1
        assert ec.field2 == {"a": "b"}


class TestRegisterBookkeeping:
    def test_appends_addr_to_system_addrs_list(self, db, isolate_registry):
        addr = insert_addr(db)
        item = Knowledge("desc", "content", "name", addr)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)
        assert addr in SYSTEM_ADDRS_LIST

    def test_registers_callable_under_addr_register(self, db, isolate_registry):
        addr = insert_addr(db)
        item = Knowledge("desc", "content", "name", addr)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)
        assert addr in ADDR_REGISTER
        assert callable(ADDR_REGISTER[addr])

    def test_returns_the_item_unchanged(self, db, isolate_registry):
        addr = insert_addr(db)
        item = Knowledge("desc", "content", "name", addr)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            returned = register(item)
        assert returned is item

    def test_duplicate_addr_registered_twice_appears_twice_in_list(self, db, isolate_registry):
        addr = insert_addr(db)
        item_a = Knowledge("desc a", "content a", "name a", addr)
        item_b = Knowledge("desc b", "content b", "name b", addr)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item_a)
            register(item_b)
        assert SYSTEM_ADDRS_LIST.count(addr) == 2


# Positive dispatch tests: verify register() stores a callable that dispatches
# through REGISTERERS_REGISTRY to the registerer for the concrete item type.
class TestRegisterDispatch:
    @pytest.mark.parametrize(
        "item_type",
        [Knowledge, Executable, Results, Masters, Slaves, Cronjob, Rmt, EventConsumers],
    )
    def test_register_dispatches_to_correct_function(
        self, db, item_type, monkeypatch
    ):
        addr = insert_addr(db)

        if item_type is Knowledge:
            item = Knowledge("desc", "content", "name", addr)
        elif item_type is Executable:
            item = Executable("desc", "body", "header", "name", addr)
        elif item_type is Results:
            item = Results("content", {"k": "v"}, "name", addr, True)
        elif item_type is Masters:
            result_addr = insert_result(db)
            item = Masters("instr", result_addr, [], "name", addr)
        elif item_type is Slaves:
            result_addr = insert_result(db)
            item = Slaves(None, "instr", result_addr, [], "general", addr)
        elif item_type is Cronjob:
            item = Cronjob("once", 60, "some_action", {"a": 1}, addr)
        elif item_type is Rmt:
            item = Rmt("START -> (instruction='x') -> END", "desc", "name", addr)
        else:
            item = EventConsumers("path", "execute_slave", "react", "general", addr)

        spy = MagicMock()
        key = str(type(item))
        old_registerer = registry_mod.REGISTERERS_REGISTRY[key]
        monkeypatch.setitem(registry_mod.REGISTERERS_REGISTRY, key, spy)

        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)

        ADDR_REGISTER[addr]()
        spy.assert_called_once_with(item, db)
        assert ADDR_REGISTER[addr] is not None


class TestRegisterKnowledgeSQL:
    def test_inserts_into_knowledge_and_vector_ops(self, db):
        addr = insert_addr(db)
        item = Knowledge("a description", "the content", "k_name_x", addr)
        register_knowledge(item, db)
        content = db.execute_fetchval("SELECT content FROM knowledge WHERE addr = %s", (addr,))
        assert content == "the content"

    def test_inserts_name_row(self, db):
        addr = insert_addr(db)
        item = Knowledge("a description", "the content", "k_name_y", addr)
        register_knowledge(item, db)
        name = db.execute_fetchval("SELECT name FROM names WHERE addr = %s", (addr,))
        assert name == "k_name_y"


class TestRegisterExecutableSQL:
    def test_inserts_into_executables(self, db):
        addr = insert_addr(db)
        item = Executable("a description", "print(1)", "a header", "e_name_x", addr)
        register_executable(item, db)
        body = db.execute_fetchval("SELECT body FROM executables WHERE addr = %s", (addr,))
        assert body == "print(1)"

    def test_inserts_name_row(self, db):
        addr = insert_addr(db)
        item = Executable("a description", "print(1)", "a header", "e_name_y", addr)
        register_executable(item, db)
        name = db.execute_fetchval("SELECT name FROM names WHERE addr = %s", (addr,))
        assert name == "e_name_y"


class TestRegisterResultSQL:
    def test_inserts_into_results(self, db):
        addr = insert_addr(db)
        item = Results("some content", {"k": "v"}, "r_name_x", addr, True)
        register_result(item, db)
        content = db.execute_fetchval("SELECT content_str FROM results WHERE addr = %s", (addr,))
        assert content == "some content"
        meta = db.execute_fetchval("SELECT metadata FROM results WHERE addr = %s", (addr,))
        assert meta == {"k": "v"}
        ready = db.execute_fetchval("SELECT ready FROM results WHERE addr = %s", (addr,))
        assert ready is True


class TestRegisterMasterSQL:
    def test_inserts_into_masters(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_x", addr)
        register_master(item, db)
        instruction = db.execute_fetchval("SELECT instruction FROM masters WHERE addr = %s", (addr,))
        assert instruction == "do the thing"

    def test_inserts_master_req_rows_for_each_dep(self, db):
        result_addr = insert_result(db)
        dep1 = insert_result(db)
        dep2 = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [dep1, dep2], "m_name_y", addr)
        register_master(item, db)
        deps = db.execute("SELECT req_addr FROM master_req WHERE master_addr = %s ORDER BY req_addr", (addr,)).fetchall()
        assert sorted(d[0] for d in deps) == sorted([dep1, dep2])

    def test_no_deps_inserts_no_master_req_rows(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_z", addr)
        register_master(item, db)
        deps = db.execute("SELECT req_addr FROM master_req WHERE master_addr = %s", (addr,)).fetchall()
        assert deps == []

    def test_inserts_name_row_when_name_given(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_w", addr)
        register_master(item, db)
        name = db.execute_fetchval("SELECT name FROM names WHERE addr = %s", (addr,))
        assert name == "m_name_w"


class TestRegisterSlavesSQL:
    def test_inserts_into_slaves(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [], "general", addr)
        register_slaves(item, db)
        instruction = db.execute_fetchval("SELECT instruction FROM slaves WHERE addr = %s", (addr,))
        assert instruction == "do the sub-thing"

    def test_inserts_slave_req_rows_for_each_dep(self, db):
        result_addr = insert_result(db)
        dep1 = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [dep1], "general", addr)
        register_slaves(item, db)
        deps = db.execute("SELECT req_addr FROM slave_req WHERE slave_addr = %s", (addr,)).fetchall()
        assert [d[0] for d in deps] == [dep1]

    def test_no_deps_inserts_no_slave_req_rows(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [], "general", addr)
        register_slaves(item, db)
        deps = db.execute("SELECT req_addr FROM slave_req WHERE slave_addr = %s", (addr,)).fetchall()
        assert deps == []


class TestRegisterCronjobSQL:
    def test_once_type_inserts_into_cronjob_once(self, db):
        addr = insert_addr(db)
        item = Cronjob("once", 60, "some_action", {"a": 1}, addr)
        register_cronjob(item, db)
        name = db.execute_fetchval("SELECT name FROM cronjob_once WHERE addr = %s", (addr,))
        assert name == "some_action"

    def test_loop_type_inserts_into_cronjob_loop(self, db):
        addr = insert_addr(db)
        item = Cronjob("loop", 60, "some_action", {"a": 1}, addr)
        register_cronjob(item, db)
        name = db.execute_fetchval("SELECT name FROM cronjob_loop WHERE addr = %s", (addr,))
        assert name == "some_action"


class TestRegisterEventConsumerSQL:
    def test_call_rmt_action_type(self, db):
        rmt_addr = insert_rmt_template(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "call_rmt", rmt_addr, {"x": 1}, addr)
        register_event_consumer(item, db)
        row = db.execute("SELECT rmt_addr, args FROM event_call_rmt WHERE addr = %s", (addr,)).fetchone()
        assert row[0] == rmt_addr

    def test_execute_slave_action_type(self, db):
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "execute_slave", "react to this", "general", addr)
        register_event_consumer(item, db)
        row = db.execute("SELECT instruction, scope FROM event_call_execute_slave WHERE addr = %s", (addr,)).fetchone()
        assert row[0] == "react to this"
        assert row[1] == "general"

    def test_fill_result_action_type(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "fill_result", result_addr, "got ${{data}}", addr)
        register_event_consumer(item, db)
        row = db.execute("SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s", (addr,)).fetchone()
        assert row[0] == result_addr
        assert row[1] == "got ${{data}}"

    def test_inserts_base_event_consumers_row(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.some.path", "fill_result", result_addr, "s", addr)
        register_event_consumer(item, db)
        event_path, action_type = db.execute(
            "SELECT event_path, action_type FROM event_consumers WHERE addr = %s", (addr,)
        ).fetchone()
        assert event_path == "evt.some.path"
        assert action_type == "fill_result"


class TestBaseStateStartup:
    def test_calls_addr_register_for_each_missing_addr(self, db):
        called = []
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", [123456789]), \
             patch.object(base_state_main, "ADDR_REGISTER", {123456789: lambda: called.append(123456789)}):
            base_state_main.startup()
        assert called == [123456789]

    def test_does_not_call_addr_register_for_addrs_already_present(self, db):
        existing_addr = insert_addr(db)
        called = []
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", [existing_addr]), \
             patch.object(base_state_main, "ADDR_REGISTER", {existing_addr: lambda: called.append(existing_addr)}):
            base_state_main.startup()
        assert called == []

    def test_successful_startup_does_not_log_fatal(self, db):
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", []), \
             patch.object(base_state_main, "ADDR_REGISTER", {}), \
             patch.object(base_state_main, "log_json") as mock_log:
            base_state_main.startup()
        # Should not call log_json with fatal status
        assert not any(call[0][0].get("status") == "fatal" for call in mock_log.call_args_list if call[0])

    def test_startup_does_not_raise(self, db):
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", []), \
             patch.object(base_state_main, "ADDR_REGISTER", {}):
            base_state_main.startup()  # should not raise

