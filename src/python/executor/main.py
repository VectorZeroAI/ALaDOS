#!/usr/bin/env python3
import re
import threading
import tomllib
import traceback
from types import FunctionType
from typing import Sequence

from psycopg.types.json import Jsonb

from ..executor.exceptions import ContextLimitExceededError, ParadoxDetected
from ..executor.execute_tool import execute_tool
from ..interrupts.main import interruptable
from ..queue import global_interrupt_queue
from ..sceduler.goal_stack.context import HEADERS_REGISTRY, resolve_loads
from ..sceduler.main import slave_addr_to_instr
from ..types import ReferenceTo
from ..utils.config_dir_resolver import config_dir_resolver
from ..utils.conn_factory import Conn, conn_factory
from ..utils.llm_to_json import llm_to_json
from ..utils.logger import log_json
from ..utils.uqueue import Uqueue
from . import embedder
from .api_calls_handler import api_calls_block
from .cronjobs import main as cronjob_handler
from .queue import embedder_queue, executor_interrupt_queue, executor_queue
from .types import Api, InstrJson, ToolCall, ToolCallsBlock, _ExecToolMetaData

config_dir = config_dir_resolver()
config_file = config_dir / "executor.toml"
config = tomllib.loads(config_file.read_text())

class ExecutionFailed(Exception):
    def __init__(self, message: str, call1: ToolCall,
                 call2: ToolCall, callb1: ToolCallsBlock,
                 callb2: ToolCallsBlock, error1: Exception,
                 error2: Exception,
                 ) -> None:
        super().__init__(message)
        self.call1 = call1
        self.call2 = call2
        self.callb1 = callb1
        self.callb2 = callb2
        self.error1 = error1
        self.error2 = error2

    def __str__(self) -> str:
        return f"call1: {self.call1}, call2: {self.call2}, callblock 1: {self.callb1}, callblock2: {self.callb2}, error1: {self.error1}, error2: {self.error2}"


def prepare_context_shortening_prompt(error: ContextLimitExceededError,
                                      conn: Conn,
                                      instr: InstrJson) -> str:
    """ Prepares the special prompt that would make the LLM get it all done correctly. """

    window_data = conn.execute("""
SELECT mc.window_anchor_exe, mc.window_anchor_knowledge, mc.window_size_l, mc.window_size_r
FROM slaves s
    INNER JOIN masters m ON s.master_addr = m.addr
    INNER JOIN master_context mc ON mc.addr = m.addr
WHERE s.addr = %s
                          """, (instr['slave_addr'],)).fetchone()
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
    
    loaded_items_addr = conn.execute("""SELECT ml.item_addr, vp.description
                                     FROM master_load ml 
                                        LEFT JOIN vector_ops vp ON ml.item_addr = vp.addr 
                                     WHERE master_addr = %s""", (instr['master_addr'],)).fetchall()

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




def fix_llm_response(slave: InstrJson, llm_response: str) -> ToolCallsBlock:
    llm_without_think = re.sub(r'<think>.*?</think>', '', llm_response, re.DOTALL)
    log_json({
        'type': 'llm_response',
        'status': 'abnormal',
        'reason': 'did not find any tool calls.',
        'llm_without_think': llm_without_think
    })
    match slave['scope']:
        case '_webui':
            tool_calls: ToolCallsBlock = [{
                    "tool": "user.send_message", 
                    "args": {"text": llm_without_think}
                }]

        case _:
            tool_calls: ToolCallsBlock = [{
                    "tool": "result.write", 
                    "args": {"text": llm_without_think}
                }]

    log_json({
        'type': 'llm_response',
        'status': 'recovered',
        'reason': 'created the new set of toolcalls from the LLM response',
        'llm_without_think': llm_without_think,
        'new_tool_calls': tool_calls
    })

    return tool_calls



@interruptable(executor_interrupt_queue, global_interrupt_queue)
def core(
        checkpoint: FunctionType,
        queue: Uqueue[int],
        apis: Sequence[Api],
        ) -> None:

    checkpoint()
    conn = conn_factory()

    def execute_llm_call(prompt: str) -> str:
        while True:
            try:
                checkpoint()
                llm_output_new = api_calls_block(apis, checkpoint, prompt)
                if llm_output_new is not None:
                    break
                else:
                    continue
            except ContextLimitExceededError as e:
                llm_output = execute_llm_call(prepare_context_shortening_prompt(e, conn, instr))
                tool_calls = llm_to_json(llm_output)
                execute_tool_calls(tool_calls)
                try:
                    nonlocal instr
                    nonlocal str_instr
                    instr = slave_addr_to_instr(slave_addr, conn)
                    str_instr = " ".join([f"CONTEXT: {instr["context"]} CONTEXT END", f"INSTRUCTION: {instr["instruction"]} INSTRUCTION END"])
                except Exception as e:
                    print(f"CONTEXT RESOLUTION FAILED {e}. Making the slave failed and moving on.")
                    conn.execute("""
            UPDATE results SET status = 'error' FROM slaves s WHERE s.addr = %s AND addr = s.result_addr;
                                 """, (slave_addr,))
        return llm_output_new


    def execute_tool_calls(tool_calls_block: ToolCallsBlock) -> list[str]:
        nresults = []
        with conn.transaction():
            for ncall in tool_calls_block:
                checkpoint()
                try:
                    ntool_result = execute_tool(ncall, metadata_c)
                except Exception as e:
                    raise ExecutionFailed(str(None), call, ncall, tool_calls, new_calls, e, e) 
                checkpoint()
                nresults.append(ntool_result)
        return nresults

    while True:
        checkpoint()

        slave_addr = queue.get_blocking()

        with conn.transaction():
            try:
                instr = slave_addr_to_instr(slave_addr, conn)
            except Exception as e:
                print(f"CONTEXT RESOLUTION FAILED {e}. Making the slave failed and moving on.")
                conn.execute("""
        UPDATE results SET status = 'error' FROM slaves s WHERE s.addr = %s AND addr = s.result_addr;
                             """, (slave_addr,))
                continue
            try:
                str_instr = " ".join([f"CONTEXT: {instr["context"]} CONTEXT END", f"INSTRUCTION: {instr["instruction"]} INSTRUCTION END"])
                print(str_instr)

                llm_response = execute_llm_call(str_instr)
                
                checkpoint()
                log_json({
                    'type': 'llm_response',
                    'status': 'normal',
                    'response': llm_response
                })
                try:
                    tool_calls: ToolCallsBlock = llm_to_json(llm_response)
                except ValueError:
                    tool_calls = fix_llm_response(instr, llm_response)

                results = []

                metadata_c: _ExecToolMetaData = {
                        'conn': conn,
                        'master_id': instr['master_addr'],
                        '_embedder_queue': Uqueue[ReferenceTo](),
                        'slave_id': slave_addr,
                        'context_limit': config.get('context_limit', 40000)
                        }

                for call in tool_calls:
                    checkpoint()
                    try:
                        with conn.transaction():
                            print(f"executing tool call {call}")
                            tool_result = execute_tool(call, metadata_c)

                    except ParadoxDetected as e:
                        # TODO : When reusable master templates are implemented
                        # Turn this into a reusable master template
                        # With all the required logic for a robust paradox resolver.
                        items = list(e.items)
                        n_items =  []
                        for i in items:
                            if isinstance(i, str):
                                n_items.append(i)
                        for i in n_items:
                            items.remove(i)

                        addrs_items: Sequence[int] = []
                        for i in n_items:
                             addrs_items.append(conn.execute_fetchval("SELECT resolve_name(%s);", (i,)))

                        prompt = f"""
                        Your task is to resolve the following paradox in the following items.
                        Your task is to resolve the following paradox in the following items.
                        Your task is to resolve the following paradox in the following items.
                        ITEMS BEGIN: {resolve_loads({"items_addrs": addrs_items})} ITEMS END.
                        PARADOX BEGIN: {e.paradox} PARADOX END.
                        AVAILABLE TOOLS BEGIN: {HEADERS_REGISTRY['context']} AVAILABLE TOOLS END.
                            """
                        llm_response = execute_llm_call(prompt)

                        tool_calls_block = llm_to_json(llm_response)
                        with conn.transaction():
                            nresults = execute_tool_calls(tool_calls_block)
                        results.extend(nresults)

                        continue

                    except Exception as e:
                        log_json({
                            'type': 'tool',
                            'status': 'error',
                            'message': str(e),
                            'tool': call,
                            'metadata': metadata_c
                        })
                        prompt = f"""The following tool call failed for the following reason: {call}, {e}
                        Your task is to figure out what went wrong there, and create a working tool call.
                        Here is what it attempted to do "{instr['instruction']}".
                        The following is the tool call format instructions and all the valid tools:
                        """ + "\n".join(HEADERS_REGISTRY['general'])

                        llm_output_new = execute_llm_call(prompt)

                        new_calls = llm_to_json(llm_output_new)
                        log_json({
                            'type': 'tool_error_recovery',
                            'status': 'normal',
                            'new_llm_output': llm_output_new,
                            'new_tool_calls': new_calls
                        })
                        with conn.transaction():
                            nresults = execute_tool_calls(new_calls)
                        results.extend(nresults)

                        continue

                    results.append(tool_result)

                result_str = "\n".join([str(r) for r in results])

                checkpoint()
                conn.execute("""
                SELECT new_result(%s, %s);
                             """, (result_str, instr["result_addr"]))

                items = metadata_c['_embedder_queue'].get_all()
                for i in items:
                    embedder_queue.put(i)

            except ExecutionFailed as e:
                with conn.transaction():
                    conn.execute("""
                    UPDATE results SET status = 'error', status_inf = %s WHERE addr = %s;
                                 """, (Jsonb({
                                     'tool_call': e.call1,
                                     'tool_call_recovery': e.call2,
                                     'tool_calls_block': e.callb1,
                                     'tool_calls_block_recovery': e.callb2,
                                     'error_original': str(e.error1),
                                     'error_from_recovery': str(e.error2)
                                 }), instr['result_addr']))
            
            except Exception as e:
                print(f"CORE THREAD ERROR CAUGHT: {e}, with args {e.args}, and traceback {traceback.print_tb(e.__traceback__)} REVERTING TRANSACTION")
                print("MOVING ON TO THE NEXT THING. PRODUCING INVALID STATE IN THE PROCESS")

def core_thread(queue: Uqueue, apis: Sequence[Api]) -> None:
    try:
        core(queue, apis) # This is valid, because checkpoint is part of the interruptable decorator, and is injected at decoration time. 
    except Exception as e:
        print(f"CORE THREAD ERRORED OUT: {e}")
        raise RuntimeError(f"CORE THREAD FAILED: {e}") from e

def startup() -> None:
    """ The startup function that starts up the whole executor system """


    apis = []
    for i in config['apis']:
        i['rate_limit'] = 2
        i['lock'] = threading.Lock()
        i['consecutive_ratelimits'] = 0
        i['rate_limited_until'] = 0.0
        apis.append(i)

    for _ in range(config["cores_number"]):
        threading.Thread(
                target=core_thread,
                args=(executor_queue, apis),
                daemon=True
                ).start()
    print("startup of the executor finished")

    embedder.setup()
    cronjob_handler.setup()


"""

config structure is the following: 
    executor.toml,
    sceduler.toml,
    db.toml,
    permissions.toml
    main.toml
    IO.toml
    etc.


An example of an expected executor.toml is:

´´´
cores_number = 12
[[apis]]
url = "http://example.com/v1/completions/example"
key = "example-api-key"
model = "IAMSTUPIDMODELEXAMPLE"
[[apis]]
url = "lazy me"
key = "lazy me"
model = sonnet
claude = true
´´´

"""
