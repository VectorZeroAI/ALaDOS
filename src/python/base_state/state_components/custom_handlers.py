#!/usr/bin/env python3
"""
The file for all the custom handlers to be pushed into the event system.
"""
from ...executor.queue import interrupt_queue
from ...interrupts.main import InterruptInvokation
from ...utils.conn_factory import async_conn_factory_raw as conn_factory
from ..registry import register
from ..types import CustomListener


async def tool_cache_invalidator():
    """ Invalidates tool cache for ToolManager of executor/execute_tool via firing an interrupt to drop the tool. """

    conn = await conn_factory()
    await conn.execute("LISTEN tool_changed;")
    async for n in conn.notifies():
        interrupt_queue.put(InterruptInvokation("invalidate_tool_cache", {"tool": n.payload}))


register(
    CustomListener(
        async_coro=tool_cache_invalidator()
    )
)
