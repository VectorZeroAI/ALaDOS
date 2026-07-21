#!/usr/bin/env python3
from datetime import datetime
import threading
import tomllib
from types import FunctionType
from typing import Sequence
import traceback

from ..sceduler.goal_stack.types import LoadsData

from ..utils.config_handlers import load_apis_from_text

from ..executor.exceptions import ContextLimitExceededError, ParadoxDetected
from ..executor.execute_tool import execute_tool
from ..interrupts.main import interruptable
from ..queue import global_interrupt_queue
from ..context.main import HEADERS_REGISTRY, resolve_loads
from ..sceduler.main import slave_addr_to_instr
from ..utils.config_dir_resolver import config_dir_resolver
from ..utils.conn_factory import Conn, conn_factory
from ..utils.llm_to_json import llm_to_json
from ..utils.logger import log_json
from ..utils.uqueue import Uqueue
from . import embedder
from .api_calls_handler import api_calls_block
from .cronjobs import main as cronjob_handler
from .queue import embedder_queue, executor_interrupt_queue, executor_queue
from .types import (Api, Instr,
                    _ExecToolMetaData,
                    ApiCallsState,
                    ContextGetState,
                    ContextShortState,
                    GetSlaveState,
                    ErrorState,
                    ExecuteState,
                    FinishState,
                    ParadoxState, 
                    State)
from .helpers import prepare_context_shortening_prompt, fix_llm_response

config_dir = config_dir_resolver()
config_file = config_dir / "executor.toml"
config = tomllib.loads(config_file.read_text())


@interruptable(executor_interrupt_queue, global_interrupt_queue)
def core(checkpoint: FunctionType, queue: Uqueue[int], apis: Sequence[Api]) -> None:
    """
The executor core, handles the execution of tasks.
Interruptable to handle more prioritised tasks then the task currently being handled.

Architecture of states:
    1. GET_SLAVE
    2. CONTEXT_GEN
    3. PREPARE (API calls and getting of tool calls) 
    4. EXECUTE (only executes tool calls)
    5. PARADOX
    6. ERROR
    7. CONTEXT_SHORTENING
    8. FINISH (Write the result)

Transitions:
    1 -> 2 -> 3 -> 4 -> 8
    3 -> 7 -> 4 -> 3
    4 -> 5 -> 4
    1 -> 6
    2 -> 6
    3 -> 6
    4 -> 6
    5 -> 6
    7 -> 6
    8 -> 6

Further documentation of the states inlined as docstrings in the match statement. 

    """

    def call_llm(str_instr: str, instr: Instr) -> tuple[str, State|None]:
        while True:
            try:
                checkpoint()
                llm_output = api_calls_block(apis, checkpoint, str_instr)
                if llm_output is not None:
                    return (llm_output, None)
                else:
                    continue
            except ContextLimitExceededError as e:
                print("CONTEXT LIMIT EXCEEDED ERROR")
                log_json({
                    'type': 'core',
                    'status': 'error',
                    'message': str(e),
                    'state': str(state.tag)
                })
                return ('', ContextShortState(instr.slave_addr, e, instr))
            except Exception as e:
                print(f"FATAL ERROR {e}, TRACEBACK: {traceback.format_exception(e)}")
                log_json({
                    'type': 'core', 
                    'status': 'fatal',
                    'message': str(e),
                    'state': str(state.tag),
                    'traceback': traceback.format_exception(e)
                })
                return ('', ErrorState(instr.slave_addr))

    conn: Conn = conn_factory()

    state_queue = Uqueue[State]()

    def set_next_state(state: State) -> None:
        state_queue.prepend(state)

    def add_state(state: State) -> None:
        state_queue.put(state)

    def set_error_state(state: ErrorState) -> None:
        state_queue.clear()
        state_queue.put(state)

    add_state(GetSlaveState())

    while True:
        state: State = state_queue.get()

        match state:
            case GetSlaveState():
                """ This is just the state of awaiting next task. """
                slave_addr = queue.get()
                set_next_state(ContextGetState(slave_addr))

            case ContextGetState():
                """
                This state gets the context using the slave_addr_to_instr method from the context stack. 

                Sets the occ_timestamp AND the finish flag and passes them on.
                The core idea is that if the API calls state or Execution state were called on a different
                path from the normal execution path, the finish would be false,
                and thus anouther path for that very same slave can be achieved.
                """
                curr = state
                try:
                    with conn.transaction():
                        instr = slave_addr_to_instr(curr.slave_addr, conn)
                except Exception as e:
                    log_json({
                        'type': 'core',
                        'status': 'fatal',
                        'message': str(e),
                        'state': str(state)
                    })
                    

                    set_error_state(ErrorState(curr.slave_addr))
                    continue


                str_instr = " ".join([f"CONTEXT: {instr.context} CONTEXT END", f"INSTRUCTION: {instr.instruction} INSTRUCTION END"])

                set_next_state(ApiCallsState(str_instr, instr, datetime.now(), finish=True))

            case ApiCallsState():
                """
                This is the API calls state, that just calls API and creates the tool calls block.
                Passes occ_timestamp through. 
                Passes the finish flag through.
                """
                curr = state
                try:
                    llm_output, next_state = call_llm(curr.str_instr, curr.instr)
                    if next_state:
                        set_next_state(next_state)
                        continue
                    print(f"LLM OUTPUT: {llm_output}")

                except Exception as e:
                    log_json({
                        'type': 'core',
                        'status': 'fatal',
                        'message': str(e),
                        'state': str(state.tag)
                    })
                    set_error_state(ErrorState(curr.instr.slave_addr))
                    continue

                try:
                    tool_calls = llm_to_json(llm_output)
                except ValueError as e:
                    log_json({
                        'type': 'core',
                        'status': 'error',
                        'message': str(e),
                        'state': str(state.tag)
                    })
                    tool_calls = fix_llm_response(curr.instr, llm_output)

                set_next_state(ExecuteState(tool_calls, curr.instr, curr.occ_timestamp, finish=curr.finish))


            case ExecuteState():
                """
                This is the execution state, it executes the tool calls block,
                and has a big retry mashine.
                occ_timestamp gets passed through from the GetContextState.
                """
                curr = state

                metadata_c = _ExecToolMetaData(
                        curr.instr.master_addr,
                        conn,
                        curr.instr.slave_addr,
                        config.get('context_limit', 40000),
                        curr.occ_timestamp
                    )

                results = []
                
                original_tool_calls_amount = len(curr.tool_calls)

                with conn.transaction():
                    for i, call in enumerate(curr.tool_calls):
                        checkpoint()
                        try:
                            with conn.transaction():
                                results.append(execute_tool(call, metadata_c))

                        except ParadoxDetected as e:
                            paradox_e: ParadoxDetected = e
                            set_next_state(ParadoxState(paradox_e, curr.instr, datetime.now()))
                            break

                        except Exception as e:
                            log_json({
                                'type': 'core',
                                'subtype': 'tool',
                                'status': 'error',
                                'message': str(e),
                                'state': str(state.tag),
                                'action': 'attempting recovery'
                            })
                            curr.error_count += 1

                            if curr.error_count > original_tool_calls_amount:
                                log_json({
                                    'type': 'core',
                                    'subtype': 'tool', 
                                    'status': 'fatal',
                                    'message': 'Recursive tool call errors detected!',
                                    'state': str(state.tag)
                                })
                                set_error_state(ErrorState(curr.instr.slave_addr))
                                break

                            prompt = f"""The following tool call failed for the following reason: {call}, {e}
                            Your task is to figure out what went wrong there, and create a working tool call.
                            Here is what it attempted to do "{curr.instr.instruction}".
                            The following is the tool call format instructions and all the valid tools:
                            """ + "\n".join(HEADERS_REGISTRY['general'])

                            n_llm_out, n_state = call_llm(prompt, curr.instr)
                            if n_state:
                                set_next_state(n_state)
                                break
                            try:
                                n_tool_calls = llm_to_json(n_llm_out)
                            except ValueError:
                                log_json({
                                    'type': 'core',
                                    'status': 'error',
                                    'message': str(e),
                                    'state': str(state.tag)
                                })
                                n_tool_calls = fix_llm_response(curr.instr, n_llm_out)
                            for j, ntc in enumerate(n_tool_calls, 1):
                                curr.tool_calls.insert(i+j, ntc)
                         ## NOTE : This inserts the new tool calls in,
                         ## wich is way better then repeating all the mashienery of execution.
                    else:
                        ## NOTE : This only happens if the for-loop wasnt broken out,
                        ## Wich means that all the routes that go to entirely different states actually
                        ## work without this one finishing prepaturely. 
                        if curr.finish:
                            set_next_state(FinishState(results, metadata_c, curr.instr))


            case FinishState():
                curr = state

                result_str = "\n".join(curr.results)

                checkpoint()

                conn.execute("""
                SELECT new_result(%s, %s);
                             """, (result_str, curr.instr.result_addr))

                items = curr.metadata_c._embedder_queue.get_all()
                for i in items:
                    embedder_queue.put(i)

                set_next_state(GetSlaveState())

            case ContextShortState():
                curr = state
                set_next_state(ApiCallsState(prepare_context_shortening_prompt(curr.error, conn, curr.instr), curr.instr, datetime.now()))
                add_state(ContextGetState(curr.instr.slave_addr))
                """
                This means that first it will execute the entire chain of the context shortening tool calls and API calls,
                and then it will execute the normal path again from ContextGetState.
                """

            case ParadoxState():
                ## TODO : When reusable master templates are implemented
                ## Turn this into a reusable master template
                ## With all the required logic for a robust paradox resolver.

                curr = state

                items = list(curr.paradox_e.items)
                n_items =  []
                for i in reversed(items):
                    if isinstance(i, str):
                        n_items.append(i)
                        items.remove(i)

                assert items is list[int]

                addrs_items: list[int] = items
                for i in n_items:
                     addrs_items.append(conn.execute_fetchval("SELECT resolve_name(%s);", (i,)))

                prompt = f"""
                Your task is to resolve the following paradox in the following items.
                Your task is to resolve the following paradox in the following items.
                Your task is to resolve the following paradox in the following items.
                ITEMS: {resolve_loads(LoadsData(addrs_items), conn)} ITEMS END.
                PARADOX: {curr.paradox_e.paradox} PARADOX END.
                AVAILABLE TOOLS: {HEADERS_REGISTRY['context']} AVAILABLE TOOLS END.
                    """

                set_next_state(ApiCallsState(prompt, curr.instr, curr.time))
                add_state(ContextGetState(curr.instr.slave_addr))

            case ErrorState():
                curr = state
                with conn.transaction():
                    conn.execute("""
            UPDATE results SET status = 'error' FROM slaves s WHERE s.addr = %s AND s.addr = s.result_addr;
                                 """, (curr.slave_addr,))
                set_next_state(GetSlaveState())

def core_thread(queue: Uqueue, apis: Sequence[Api]) -> None:
    try:
        core(queue, apis) # This is valid, because checkpoint is part of the interruptable decorator, and is injected at decoration time. 
    except Exception as e:
        print(f"CORE THREAD ERRORED OUT: {e}")
        raise RuntimeError(f"CORE THREAD FAILED: {e}") from e

def startup() -> None:
    """ The startup function that starts up the whole executor system """
    
    apis = load_apis_from_text(config_file.read_text())

    for _ in range(config["cores_number"]):
        threading.Thread(
                target=core_thread,
                args=(executor_queue, apis),
                daemon=True
                ).start()

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
