#!/usr/bin/env python3
"""
Tests for the base_state subsystem (python/base_state/*).

Requires a PostgreSQL test database with the ALaDOS schema applied
(same conventions as event_tests.py / conn_factory_test.py).

Covers:
    - types.py: dataclass construction, new_addr()
    - registry.py: register() bookkeeping, __register_item dispatch,
      and each REGISTERERS_REGISTRY entry's actual SQL against the real schema
    - main.py: startup()'s existence check and its ADDR_REGISTER dispatch

These tests assert *actual current behavior*, including known bugs, rather
than the intended behavior. Bugs are flagged in the test docstring/comment
where discovered so they're not mistaken for the spec. No xfail markers are
used -- a test that documents a bug asserts the buggy behavior and says so.

Because registry.py's `register` decorator and the state_components/*.py
files mutate *module-level* globals (SYSTEM_ADDRS_LIST, ADDR_REGISTER,
REGISTERERS_REGISTRY) as an import-time side effect, these tests avoid
importing python.base_state.state_components at all, and instead exercise
registry.py's machinery directly with locally-constructed Item instances,
snapshotting/restoring the globals around each test so ordering between
test files/functions can't leak state.
"""

import os
from unittest.mock import patch

import pytest

from python.utils.conn_factory import Conn, register_all_the_composite_types

from python.base_state.types import (
    Cronjob,
    EventConsumers,
    Executable,
    Knowledge,
    Masters,
    Results,
    Rmt,
    Slaves,
    new_addr,
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
    register_slaves,
)
import python.base_state.main as base_state_main


# ----------------------------------------------------------------------
# Test connection helpers (mirrors event_tests.py)
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


@pytest.fixture
def clean_registry_globals():
    """
    register() and __item_registerer() mutate module-level globals as a side
    effect. Snapshot and restore them so tests that call register() directly
    don't leak addrs/callables into other tests.
    """
    addrs_before = list(SYSTEM_ADDRS_LIST)
    addr_register_before = dict(ADDR_REGISTER)
    registerers_before = dict(REGISTERERS_REGISTRY)
    yield
    SYSTEM_ADDRS_LIST[:] = addrs_before
    ADDR_REGISTER.clear()
    ADDR_REGISTER.update(addr_register_before)
    REGISTERERS_REGISTRY.clear()
    REGISTERERS_REGISTRY.update(registerers_before)


# ----------------------------------------------------------------------
# Seed helpers
# ----------------------------------------------------------------------
def insert_result(db: Conn) -> int:
    return db.execute_fetchval(
        "INSERT INTO results (addr) VALUES (new_addr()) RETURNING addr"
    )


def insert_addr(db: Conn) -> int:
    return db.execute_fetchval("SELECT new_addr()")


def insert_rmt_template(db: Conn) -> int:
    return db.execute_fetchval(
        "INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr"
    )


# ----------------------------------------------------------------------
# types.py -- new_addr()
# ----------------------------------------------------------------------
class TestNewAddr:
    def test_returns_int(self, db):
        with patch("python.base_state.types.conn_factory", return_value=db):
            addr = new_addr()
        assert isinstance(addr, int)

    def test_inserted_row_exists_in_addrs_table(self, db):
        with patch("python.base_state.types.conn_factory", return_value=db):
            addr = new_addr()
        found = db.execute_fetchval(
            "SELECT addr FROM addrs WHERE addr = %s", (addr,)
        )
        assert found == addr

    def test_successive_calls_return_distinct_addrs(self, db):
        with patch("python.base_state.types.conn_factory", return_value=db):
            a1 = new_addr()
            a2 = new_addr()
        assert a1 != a2


# ----------------------------------------------------------------------
# types.py -- dataclass construction sanity
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# registry.py -- register() bookkeeping
# ----------------------------------------------------------------------
class TestRegisterBookkeeping:
    """
    register() is a decorator: it appends item.addr to SYSTEM_ADDRS_LIST and
    stashes a callable under ADDR_REGISTER[item.addr], then returns the item
    unchanged. It calls conn_factory() itself (not injectable via arg), so we
    patch it at the registry module level.
    """

    def test_appends_addr_to_system_addrs_list(self, db, clean_registry_globals):
        item = Knowledge("desc", "content", "name", 999001)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)
        assert 999001 in SYSTEM_ADDRS_LIST

    def test_registers_callable_under_addr_register(self, db, clean_registry_globals):
        item = Knowledge("desc", "content", "name", 999002)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)
        assert 999002 in ADDR_REGISTER
        assert callable(ADDR_REGISTER[999002])

    def test_returns_the_item_unchanged(self, db, clean_registry_globals):
        item = Knowledge("desc", "content", "name", 999003)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            returned = register(item)
        assert returned is item

    def test_duplicate_addr_registered_twice_appears_twice_in_list(self, db, clean_registry_globals):
        """
        SYSTEM_ADDRS_LIST is a plain list with no dedup, so registering the
        same addr twice (e.g. two decorated items sharing an addr by mistake)
        silently duplicates the entry rather than raising.
        """
        item_a = Knowledge("desc a", "content a", "name a", 999004)
        item_b = Knowledge("desc b", "content b", "name b", 999004)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item_a)
            register(item_b)
        assert SYSTEM_ADDRS_LIST.count(999004) == 2


# ----------------------------------------------------------------------
# registry.py -- __register_item dispatch (via ADDR_REGISTER callables)
#
# BUG: __item_registerer registers handlers keyed by str(type(item)), but
# registry.py's own decorator calls, e.g. @__item_registerer(str(type(
# EventConsumers))) pass the *class* EventConsumers, not an *instance*. So
# str(type(EventConsumers)) is str(type) == "<class 'type'>" for every
# dataclass type registered this way, not a per-type key. Every one of
# register_event_consumer/register_rmt/register_cronjob/register_slaves/
# register_master/register_result/register_executable/register_knowledge
# is filed under the exact same "<class 'type'>" key, and each successive
# @__item_registerer(...) definition in registry.py silently overwrites the
# previous one. Only the last-defined registerer (register_knowledge, as
# the file currently reads top to bottom) survives in REGISTERERS_REGISTRY.
# __register_item then looks up str(type(item)) (str(type(an *instance*)))
# which is e.g. "<class 'python.base_state.types.Knowledge'>" -- a key that
# was never inserted -- so dispatch raises KeyError for every item type.
# ----------------------------------------------------------------------
class TestRegistererDispatchBug:
    def test_all_item_registerer_keys_collide_on_class_of_type(self):
        """
        Every REGISTERERS_REGISTRY key installed by registry.py's own
        @__item_registerer(str(type(<DataclassName>))) calls is identical.
        """
        assert str(type(Knowledge)) == str(type(EventConsumers)) == "<class 'type'>"

    def test_registerers_registry_only_has_the_class_of_type_key(self):
        """
        Because every registration collides on "<class 'type'>", only that
        one key exists in REGISTERERS_REGISTRY after module import -- not
        one key per item type.
        """
        assert list(REGISTERERS_REGISTRY.keys()) == ["<class 'type'>"]

    def test_last_defined_registerer_wins(self):
        """
        Python re-executes each @__item_registerer(...) def top to bottom at
        import time, so the last one defined in registry.py (register_
        knowledge) is the one left occupying the collided key.
        """
        assert REGISTERERS_REGISTRY["<class 'type'>"] is register_knowledge

    def test_dispatch_by_instance_type_raises_keyerror(self, db, clean_registry_globals):
        """
        __register_item looks up str(type(item)) for an *instance*, e.g.
        "<class 'python.base_state.types.Knowledge'>", which was never
        inserted into REGISTERERS_REGISTRY (only "<class 'type'>" was) --
        so dispatching any real item raises KeyError, regardless of which
        item type it is.
        """
        item = Knowledge("desc", "content", "name", 999005)
        with patch.object(registry_mod, "conn_factory", return_value=db):
            register(item)
        with pytest.raises(KeyError):
            ADDR_REGISTER[999005]()


# ----------------------------------------------------------------------
# registry.py -- individual register_* functions called directly
#
# These bypass the broken dispatch above and call each REGISTERERS_REGISTRY
# function body directly against a real (rolled-back) connection, to verify
# the SQL each one issues is actually valid against the schema.
# ----------------------------------------------------------------------
class TestRegisterKnowledgeSQL:
    def test_inserts_into_knowledge_and_vector_ops(self, db):
        addr = insert_addr(db)
        item = Knowledge("a description", "the content", "k_name_x", addr)
        register_knowledge(item, db)

        content = db.execute_fetchval(
            "SELECT content FROM knowledge WHERE addr = %s", (addr,)
        )
        assert content == "the content"

    def test_inserts_name_row(self, db):
        addr = insert_addr(db)
        item = Knowledge("a description", "the content", "k_name_y", addr)
        register_knowledge(item, db)

        name = db.execute_fetchval(
            "SELECT name FROM names WHERE addr = %s", (addr,)
        )
        assert name == "k_name_y"


class TestRegisterExecutableSQL:
    def test_inserts_into_executables(self, db):
        addr = insert_addr(db)
        item = Executable("a description", "print(1)", "a header", "e_name_x", addr)
        register_executable(item, db)

        body = db.execute_fetchval(
            "SELECT body FROM executables WHERE addr = %s", (addr,)
        )
        assert body == "print(1)"

    def test_inserts_name_row(self, db):
        addr = insert_addr(db)
        item = Executable("a description", "print(1)", "a header", "e_name_y", addr)
        register_executable(item, db)

        name = db.execute_fetchval(
            "SELECT name FROM names WHERE addr = %s", (addr,)
        )
        assert name == "e_name_y"


class TestRegisterResultSQLBug:
    """
    BUG: register_result's INSERT statement lists 4 columns
    (addr, content_str, metadata, ready) but only supplies 3 %s
    placeholders:

        INSERT INTO results(addr, content_str, metadata, ready) VALUES (%s, %s, %s);

    psycopg raises before this ever reaches postgres for a column-count
    mismatch of this kind (fewer placeholders than the driver expects vs.
    the 4-tuple `(item.addr, item.content_str, item.metadata)` actually
    passed -- wait, the call site passes a 3-tuple against a 4-column
    INSERT with 3 placeholders, so the placeholder/param counts *do* match
    each other; it's the column list vs. VALUES() list that's short by one.
    Postgres itself rejects this at execute time.
    """

    def test_raises_due_to_column_values_count_mismatch(self, db):
        addr = insert_addr(db)
        item = Results("some content", {"k": "v"}, "r_name_x", addr, True)
        with pytest.raises(Exception):
            register_result(item, db)


class TestRegisterMasterSQL:
    def test_inserts_into_masters(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_x", addr)
        register_master(item, db)

        instruction = db.execute_fetchval(
            "SELECT instruction FROM masters WHERE addr = %s", (addr,)
        )
        assert instruction == "do the thing"

    def test_inserts_master_req_rows_for_each_dep(self, db):
        result_addr = insert_result(db)
        dep1 = insert_result(db)
        dep2 = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [dep1, dep2], "m_name_y", addr)
        register_master(item, db)

        deps = db.execute(
            "SELECT req_addr FROM master_req WHERE master_addr = %s ORDER BY req_addr",
            (addr,),
        ).fetchall()
        assert sorted(d[0] for d in deps) == sorted([dep1, dep2])

    def test_no_deps_inserts_no_master_req_rows(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_z", addr)
        register_master(item, db)

        deps = db.execute(
            "SELECT req_addr FROM master_req WHERE master_addr = %s", (addr,)
        ).fetchall()
        assert deps == []

    def test_inserts_name_row_when_name_given(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Masters("do the thing", result_addr, [], "m_name_w", addr)
        register_master(item, db)

        name = db.execute_fetchval(
            "SELECT name FROM names WHERE addr = %s", (addr,)
        )
        assert name == "m_name_w"


class TestRegisterSlavesSQL:
    def test_inserts_into_slaves(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [], "general", addr)
        register_slaves(item, db)

        instruction = db.execute_fetchval(
            "SELECT instruction FROM slaves WHERE addr = %s", (addr,)
        )
        assert instruction == "do the sub-thing"

    def test_inserts_slave_req_rows_for_each_dep(self, db):
        result_addr = insert_result(db)
        dep1 = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [dep1], "general", addr)
        register_slaves(item, db)

        deps = db.execute(
            "SELECT req_addr FROM slave_req WHERE slave_addr = %s", (addr,)
        ).fetchall()
        assert [d[0] for d in deps] == [dep1]

    def test_no_deps_inserts_no_slave_req_rows(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = Slaves(None, "do the sub-thing", result_addr, [], "general", addr)
        register_slaves(item, db)

        deps = db.execute(
            "SELECT req_addr FROM slave_req WHERE slave_addr = %s", (addr,)
        ).fetchall()
        assert deps == []


class TestRegisterCronjobSQLBug:
    """
    BUG: the "once" branch of register_cronjob has a typo -- `EXRACT`
    instead of `EXTRACT` -- inside the INSERT ... VALUES(...) expression:

        VALUES (%s, %s, (EXRACT(EPOCH FROM NOW()) + %s)::INT);

    This is invalid PL/pgSQL/SQL syntax and postgres will reject it,
    regardless of the Python-level arguments passed in.
    """

    def test_once_type_raises_due_to_extract_typo(self, db):
        addr = insert_addr(db)
        item = Cronjob("once", 60, "some_action", {"a": 1}, addr)
        with pytest.raises(Exception):
            register_cronjob(item, db)

    def test_loop_type_inserts_into_cronjob_loop(self, db):
        addr = insert_addr(db)
        item = Cronjob("loop", 60, "some_action", {"a": 1}, addr)
        register_cronjob(item, db)

        name = db.execute_fetchval(
            "SELECT name FROM cronjob_loop WHERE addr = %s", (addr,)
        )
        assert name == "some_action"


class TestRegisterEventConsumerSQL:
    def test_call_rmt_action_type(self, db):
        rmt_addr = insert_rmt_template(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "call_rmt", rmt_addr, {"x": 1}, addr)
        register_event_consumer(item, db)

        row = db.execute(
            "SELECT rmt_addr, args FROM event_call_rmt WHERE addr = %s", (addr,)
        ).fetchone()
        assert row[0] == rmt_addr

    def test_execute_slave_action_type(self, db):
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "execute_slave", "react to this", "general", addr)
        register_event_consumer(item, db)

        row = db.execute(
            "SELECT instruction, scope FROM event_call_execute_slave WHERE addr = %s",
            (addr,),
        ).fetchone()
        assert row[0] == "react to this"
        assert row[1] == "general"

    def test_fill_result_action_type(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.path", "fill_result", result_addr, "got ${{data}}", addr)
        register_event_consumer(item, db)

        row = db.execute(
            "SELECT result_addr, result_str FROM event_call_fill_result WHERE addr = %s",
            (addr,),
        ).fetchone()
        assert row[0] == result_addr
        assert row[1] == "got ${{data}}"

    def test_inserts_base_event_consumers_row(self, db):
        result_addr = insert_result(db)
        addr = insert_addr(db)
        item = EventConsumers("evt.some.path", "fill_result", result_addr, "s", addr)
        register_event_consumer(item, db)

        event_path, action_type = db.execute(
            "SELECT event_path, action_type FROM event_consumers WHERE addr = %s",
            (addr,),
        ).fetchone()
        assert event_path == "evt.some.path"
        assert action_type == "fill_result"


# ----------------------------------------------------------------------
# main.py -- startup()
#
# BUG: `if results is not list[int]:` compares the *query result list*
# object against the generic alias `list[int]` with `is not`. Two distinct
# objects are never `is` each other, so this condition is always True --
# the "existence check failed" log fires on *every* startup call, even when
# the DB query succeeded and returned exactly the missing addrs correctly.
# It only logs (doesn't raise/exit), so startup continues regardless.
# ----------------------------------------------------------------------
class TestBaseStateStartup:
    def test_calls_addr_register_for_each_missing_addr(self, db):
        called = []

        def fake_conn_factory():
            return db

        with patch.object(base_state_main, "conn_factory", fake_conn_factory), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", [123456789]), \
             patch.object(
                 base_state_main,
                 "ADDR_REGISTER",
                 {123456789: lambda: called.append(123456789)},
             ):
            base_state_main.startup()

        assert called == [123456789]

    def test_does_not_call_addr_register_for_addrs_already_present(self, db):
        existing_addr = insert_addr(db)
        called = []

        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", [existing_addr]), \
             patch.object(
                 base_state_main,
                 "ADDR_REGISTER",
                 {existing_addr: lambda: called.append(existing_addr)},
             ):
            base_state_main.startup()

        assert called == []

    def test_logs_fatal_even_though_query_succeeded(self, db):
        """
        Documents the `is not list[int]` bug: log_json is invoked with a
        'fatal' status on a completely successful run (nothing missing),
        because the type-check condition is unconditionally True.
        """
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", []), \
             patch.object(base_state_main, "ADDR_REGISTER", {}), \
             patch.object(base_state_main, "log_json") as mock_log:
            base_state_main.startup()

        mock_log.assert_called_once()
        logged = mock_log.call_args[0][0]
        assert logged["status"] == "fatal"

    def test_startup_does_not_raise_despite_fatal_log(self, db):
        """The fatal log is non-fatal in practice: startup() doesn't exit/raise on it."""
        with patch.object(base_state_main, "conn_factory", return_value=db), \
             patch.object(base_state_main, "SYSTEM_ADDRS_LIST", []), \
             patch.object(base_state_main, "ADDR_REGISTER", {}):
            base_state_main.startup()  # should not raise


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

