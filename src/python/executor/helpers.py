#!/usr/bin/env python3

import asyncio
import json
import re
import subprocess
import threading
import time
from collections import OrderedDict
from dataclasses import asdict
from functools import partial
from tempfile import (
    NamedTemporaryFile,
    _TemporaryFileWrapper,
    gettempdir,
)
from threading import RLock
from typing import Any

from ..types import ToolCall
from ..utils.conn_factory import Conn, conn_factory
from ..utils.logger import log_json
from .exceptions import ContextLimitExceededError
from .execute_tool import HEADERS_REGISTRY, execute_syscall
from .types import CachedTool, Instr, ToolCallsBlock, _ExecToolMetaData


def prepare_context_shortening_prompt(error: ContextLimitExceededError,
                                      conn: Conn,
                                      instr: Instr) -> str:
    """ Prepares the special prompt that would make the LLM get it all done correctly. """

    window_data = conn.execute("""
    SELECT mc.window_anchor_exe, mc.window_anchor_knowledge, mc.window_size_l, mc.window_size_r
    FROM slaves s
        INNER JOIN masters m ON s.master_addr = m.addr
        INNER JOIN master_context mc ON mc.addr = m.addr
    WHERE s.addr = %s
                          """, (instr.slave_addr,)).fetchone()
    assert window_data is not None

    viewing_window_shortened = conn.execute("""
    WITH ordered AS (
        SELECT addr,
            position,
            type,
            ROW_NUMBER() OVER (ORDER BY position) AS rn FROM vector_ops
    ), anchor AS (
        SELECT rn FROM ordered WHERE addr = %s LIMIT 1
    )
    SELECT addr, o.rn
    FROM ordered o, anchor a
    WHERE o.rn BETWEEN a.rn - %s AND a.rn + %s;
                 """, ((window_data[0] if window_data[0] is not None else window_data[1]),
                        window_data[2],
                       window_data[3]
                       )).fetchall()
    viewing_window_context_list_str = []
    for i in viewing_window_shortened:
        viewing_window_context_list_str.append(f"Item at address: {i[0]}, at coordinate {i[1]}.")

    context_chunk_1 = "\n".join(viewing_window_context_list_str)
    
    loaded_items_addr = conn.execute("""
    SELECT ml.item_addr, vp.description
    FROM master_load ml 
        LEFT JOIN vector_ops vp ON ml.item_addr = vp.addr 
    WHERE master_addr = %s
                                     """, (instr.master_addr,)).fetchall()

    loaded_items_list_str = []
    for i in loaded_items_addr:
        loaded_items_list_str.append(f"Item at address {i[0]}, with description '{i[1]}' loaded.")

    context_chunk_2 = "\n".join(loaded_items_list_str)
    context = "\n\n\n".join([f"CONTEXT START: {context_chunk_1}",
                             f"{context_chunk_2} CONTEXT END.",
                             f"TOOLS REGISTRY START {HEADERS_REGISTRY['context']} TOOLS REGISTRY END.",
                             f"""INSTRUCTION START
                             Your task is to reduce the context size.
                             Evict entries you deem less important.
                             Start by shrinking the context window.
                             You may also evict loaded items.
                             If there is nothing to evict, do absolutely nothing,
                             I will go handle the work.
                             Current full context lenght: {error.len_payload}, you are only looking at a very reduced context.
                             INSTRUCTION END"""])

    return context

def fix_llm_response(slave: Instr, llm_response: str) -> ToolCallsBlock:
    llm_without_think = re.sub(r'<think>.*?</think>', '', llm_response, re.DOTALL)
    log_json({
        'type': 'llm_response',
        'status': 'abnormal',
        'reason': 'did not find any tool calls.',
        'llm_without_think': llm_without_think
    })
    match slave.scope:
        case '_webui':
            tool_calls: ToolCallsBlock = [
                ToolCall("user.send_message",
                         {"text": llm_without_think}
                     )
            ]

        case _:
            tool_calls: ToolCallsBlock = [
                ToolCall("result.write",
                         {"text": llm_without_think}
                     )
            ]

    log_json({
        'type': 'llm_response',
        'status': 'recovered',
        'reason': 'created the new set of toolcalls from the LLM response',
        'llm_without_think': llm_without_think,
        'new_tool_calls': str([asdict(tc) for tc in tool_calls])
    })

    return tool_calls


class ToolsManager:
    def __init__(self, limit: int):
        self.cache = OrderedDict[str, CachedTool]()
        self.lock = RLock()
        self.limit = limit
        self.conn = conn_factory()

        threading.Thread(target=self.invalidator_func, daemon=True).start()


    def invalidator_func(self):
        n_conn = conn_factory()
        n_conn.execute("LISTEN tool_changed;")
        for i in n_conn.notifies():
            name = i.payload
            if name in self.cache:
                self.cache.pop(name)

    def __getitem__(self, name: str, /) -> CachedTool:
        if name in self.cache:
            self.cache.move_to_end(name, last=False)
            return self.cache[name]

        func: CachedTool = self.prepare_function(name)

        if len(self.cache) > self.limit - 1:
            self.cache.popitem()

        self.cache[name] = func
        self.cache.move_to_end(name, last=False)

        return func

    def prepare_function(self, name: str) -> CachedTool:
        """
        This function loads a function from DB,
        and then builds the function in such a way that only arguments are left to fill in,
        basically readying it for instant usage.
        """
        conn = self.conn

        body = conn.execute_fetchval("""
        SELECT body FROM executables WHERE addr = resolve_name(%s);
                     """, (name,))

        tmp_file = NamedTemporaryFile("+rw", suffix=".py")
        tmp_file.write(body)

        return partial(_execute_tool, tmp_file)





def _execute_tool(file: _TemporaryFileWrapper, kwargs: dict[str, Any], _meta: _ExecToolMetaData):
    """ 
    Executes the given tool body from the DB.

    The tool is generally structured the same way as the syscall, except it gets file and not id.
    """
    if "timeout" in kwargs:
        timeout = kwargs.pop("timeout")
    else:
        timeout = 5

    kwargs_str: str = json.dumps(kwargs)

    process = subprocess.Popen(
        ["python3", gettempdir() + file.name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if process.stdin is not None:
        process.stdin.write(kwargs_str)
        process.stdin.flush()
    else:
        log_json({
            "type": "core",
            "subtype": "tool_execution",
            "status": "error",
            "msg": "Process.stdin is None, unable to write."
        })
        raise RuntimeError("Unable to execute, process.stdin is none, call the developer!")

    start = time.time()

    syscall_queue = _meta.syscalls_queue

    loop = asyncio.new_event_loop()

    stdout: str = ""
    stderr: str = ""

    while process.poll() is None:
        if time.time() - start > timeout:
            process.kill()
            process.wait()
            raise TimeoutError("Process Timed out.")

        if process.stdout:
            stdout = stdout + process.stdout.read()
        
        if process.stderr:
            stderr = stderr + process.stderr.read()

        for i in syscall_queue.get_all():
            ret = execute_syscall(i[0], _meta)
            loop.run_until_complete(
                i[1].respond(ret.encode())
            )

    loop.close()

    if not process.stdout:
        log_json({
            "type": "core",
            "subtype": "tool_execution",
            "status": "error",
            "msg": "STDOUT IS NONE"
        })
        stdout = "<Empty>"
    else:
        stdout = process.stdout.read()

    if not process.stderr:
        if process.poll() != 0:
            log_json({
                "type": "core",
                "subtype": "tool_execution",
                "status": "warning",
                "msg": "STDERR IS NONE"
            })
        stderr = "<Empty>"
    else:
        stderr = process.stderr.read()

    if process.poll() != 0:
        log_json({
            "type": "core",
            "subtype": "tool_execution",
            "status": "error",
            "msg": f"Tool failed with exit code {process.poll()}, output: {stdout} and error {stderr}."
        })
        raise RuntimeError(f"Tool failed with exit code {process.poll()}, output: {stdout} and error {stderr}.")

    return f"Executed tool, and got output: {stdout}{f"; and error output: {stderr}" if stderr else ""}."
