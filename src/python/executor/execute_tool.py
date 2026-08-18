#!/usr/bin/env python3

import asyncio
import inspect
import json
import re
import subprocess
import time
from collections import OrderedDict
from functools import partial
from tempfile import (
    NamedTemporaryFile,
    _TemporaryFileWrapper,
)
from threading import RLock
from typing import Any, Callable, ParamSpec, TypeVar, get_args

from ..types import SysCall
from ..utils.conn_factory import conn_factory, Conn
from ..utils.logger import log_json
from .types import CachedTool, ReferenceTo, SlaveScope_, SlaveScopesList, _ExecToolMetaData

TOOL_REGISTRY: dict[str, Callable] = {}
HEADERS_REGISTRY: dict[str, str] = {}

TOOL_USAGE_INSTRUCTION = """
You should output tool calls. Otherwise your response will be treated as plaintext result. 
Whenever you see the argument id, it means its ether an address, e.g. number, or a name.
When calling tools you must follow this instruction format:
[
    {
        "tool": "tool.name",
        "args": {
            "param_name": "value",
            "anouther_param_name": 123
        }

    },
    {
        ...
    },
    ...
]
"""

for i in get_args(SlaveScope_): # TODO : Maybe make this a bit nicer, IDK, maybe
    HEADERS_REGISTRY[i] = TOOL_USAGE_INSTRUCTION

# Pattern matches:
# - optional comma and whitespace before (if not first param)
# - the parameter itself: _master_addr: <type>
# - optional default value like = ...
# - optional trailing comma if it was the last param
pattern = r'(?:,\s*)?_meta\s*:\s*[^,=)]+(?:\s*=\s*[^,)]+)?(?:,\s*)?'

def remove_master_addr_param(signature_str: str) -> str:
    tmp = re.sub(pattern, '', signature_str).strip()
    return tmp


def _construct_header(func: Callable, name: str|None = None) -> str:
    signature = inspect.signature(func)
    signature_str = name or func.__name__
    signature_str = signature_str + str(signature)
    signature_str = remove_master_addr_param(signature_str)
    signature_str = "\n".join((signature_str, (func.__doc__ or "No description provided")))
    return signature_str

P = ParamSpec('P')
R = TypeVar('R')


def register_tool(name: str|None = None, scope: SlaveScopesList = ['general']):
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        TOOL_REGISTRY[name or func.__name__] = func
        header = _construct_header(func, name)
        for i in scope:
            HEADERS_REGISTRY[i] = "\n\n".join([HEADERS_REGISTRY[i], header])
        HEADERS_REGISTRY['all'] = "\n\n".join([HEADERS_REGISTRY['all'], header])

        # Special internal thingis here.
        HEADERS_REGISTRY['_webui'] = HEADERS_REGISTRY['general']
        return func
    return decorator



def execute_syscall(call: SysCall, _meta: _ExecToolMetaData) -> str:
    """ Executes a syscall from syscalls table """
    return TOOL_REGISTRY[call.tool](**call.args, _meta = _meta)



def execute_tool(call: SysCall, _meta: _ExecToolMetaData) -> str:
    """ Execute function from DB """
    return ToolsManager()[call.tool](call.args, _meta)



class ToolsManager:
    """
    Singleton class of ToolManager,
    handling the retrieval, caching and serving of tools.
    
    Is a singleton because I think its cleaner then creating it once and importing value every time.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(self, limit: int = 100):
        self.cache = OrderedDict[ReferenceTo, CachedTool]()
        self.lock = RLock()
        self.names_lock = RLock()
        self.limit = limit
        self.conn = conn_factory()

    def __getitem__(self, id: str|ReferenceTo, /) -> CachedTool:
        """
        The actually main function of the entire thing.

        This function resolves the name, and uses a LRU names cache.
        It also resolves the function itself and uses LRU functions cache. 
        TODO: Split that logic into smaller methods.

        It also handles the coersion of something like "95134" into an addr. TODO : Actually do that.
        """
        addr = self.conn.resolve_to_addr(id)

        if addr in self.cache:
            with self.lock:
                self.cache.move_to_end(addr, last=False)
                return self.cache[addr]

        func: CachedTool = self.prepare_function(addr)

        if len(self.cache) > self.limit - 1:
            with self.lock:
                self.cache.popitem()

        with self.lock:
            self.cache[addr] = func
            self.cache.move_to_end(addr, last=False)
            return func

    def invalidate(self, addr: ReferenceTo, /):
        """ Removes tool from cache. For tool changes. """
        if addr in self.cache:
            with self.lock:
                self.cache.pop(addr)

    def prepare_function(self, id: ReferenceTo) -> CachedTool:
        """
        This function loads a function from DB,
        and then builds the function in such a way that only arguments are left to fill in,
        basically readying it for instant usage.
        """
        conn = self.conn

        body = conn.execute_fetchval("""
        SELECT body FROM executables WHERE addr = %s;
                     """, (id,))

        tmp_file = NamedTemporaryFile("w+", suffix=".py")
        tmp_file.write(body)
        tmp_file.flush()

        return partial(_execute_tool, tmp_file)



def tools_changed(syscall: SysCall, result: str, conn: Conn) -> tuple[str|None, ReferenceTo|None]:
    """
    Checks what tools were created or edited via this tool call and returns the address and name of them.
    """
    changed_addr = None
    changed_name = None

    if syscall.tool == "tool_create":
        changed_name = syscall.args.get("name")

        if not isinstance(changed_name, str):
            changed_name = str(changed_name)

        changed_addr = int(result)

    if syscall.tool == "tool_edit":
        id = str(syscall.args.get("id"))
        try:
            changed_addr = int(id) # pyright: ignore # NOTE : Cause we just TRY.
        except ValueError:
            changed_addr = conn.resolve_to_addr(id)
        changed_name = id

    return (changed_name, changed_addr)


def check_invalid_syscall(syscall: SysCall,
                          changed_tools_names: list[str],
                          changed_tools_addrs: list[int],
                          conn: Conn) -> None:
    """
    Does all the invalid syscall checking and raises NotImplementedError if tool is accessed that was just changed.
    """
    
    if syscall.tool != "tool_execute":
        return
    id = str(syscall.args["id"])
    addr = conn.resolve_to_addr(id)
    if isinstance(id, str):
        name = id
    else:
        name = None
        ## NOTE : This doesnt actually get names, so theoretically, shit can still go sideways with tools cache.
        ## This should be documented and explained in the ALaDOS tools Language specification.
    if (name in changed_tools_names) or (addr in changed_tools_addrs):
        raise NotImplementedError("Usage of tools edited in the same slave is not allowed ! It must be outsourced to a different Slave !")


def _execute_tool(file: _TemporaryFileWrapper, kwargs: dict[str, Any], _meta: _ExecToolMetaData) -> str:
    """ 
    Executes the given tool body from the DB.

    The tool is generally structured the same way as the syscall, except it gets file and not id.
    """
    if "timeout" in kwargs:
        timeout = kwargs.pop("timeout")
    else:
        timeout = 5

    kwargs['slave_id'] = _meta.slave_id
    kwargs['master_id'] = _meta.master_id

    kwargs_str: str = json.dumps(kwargs)

    process = subprocess.Popen(
        ["python3", file.name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if process.stdin is not None:
        process.stdin.write(kwargs_str)
        process.stdin.close()
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
    changed_tool_names, changed_tools_addrs = _meta.changed_tools_names, _meta.changed_tools_addrs

    loop = asyncio.new_event_loop()

    stdout: str = ""
    stderr: str = ""

    while process.poll() is None:
        if time.time() - start > timeout:
            process.kill()
            process.wait()
            raise TimeoutError("Process Timed out.")

        try:
            out_chunk, err_chunk = process.communicate(timeout=0.05)
            stdout = stdout + out_chunk
            stderr = stderr + err_chunk
        except subprocess.TimeoutExpired:
            pass

        for i in syscall_queue.get_all():
            check_invalid_syscall(i[0], changed_tool_names, changed_tools_addrs, _meta.conn)
            ret = execute_syscall(i[0], _meta)
            loop.run_until_complete(
                i[1].respond(ret.encode())
            )
            name, addr = tools_changed(i[0], ret, _meta.conn)

            if name:
                changed_tool_names.append(name)
            if addr:
                changed_tools_addrs.append(addr)

    loop.close()

    out_chunk, err_chunk = process.communicate(timeout=0.3)

    stdout = stdout + out_chunk
    stderr = stderr + err_chunk

    if not stdout:
        log_json({
            "type": "core",
            "subtype": "tool_execution",
            "status": "error",
            "msg": "STDOUT IS NONE"
        })
        stdout = "<Empty>"

    if not stderr:
        if process.poll() != 0:
            log_json({
                "type": "core",
                "subtype": "tool_execution",
                "status": "warning",
                "msg": "STDERR IS NONE"
            })
        stderr = "<Empty>"

    if process.poll() != 0:
        log_json({
            "type": "core",
            "subtype": "tool_execution",
            "status": "error",
            "msg": f"Tool failed with exit code {process.poll()}, output: {stdout} and error {stderr}."
        })
        raise RuntimeError(f"Tool failed with exit code {process.poll()}, output: {stdout} and error {stderr}.")

    return f"Executed tool, and got output: {stdout}{f"; and error output: {stderr}" if stderr else ""}."


# register all the tools
# THIS IS REQUIRED ! DONT REMOVE THIS!!!
from . import builtins as __owuergnsorjgnborn  # noqa # pyright: ignore
