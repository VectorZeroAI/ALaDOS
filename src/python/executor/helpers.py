#!/usr/bin/env python3

import re

from ..utils.conn_factory import Conn
from .exceptions import ContextLimitExceededError
from .types import Instr, ToolCallsBlock, ToolCall
from .execute_tool import HEADERS_REGISTRY
from ..utils.logger import log_json

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
    
    loaded_items_addr = conn.execute("""SELECT ml.item_addr, vp.description
                                     FROM master_load ml 
                                        LEFT JOIN vector_ops vp ON ml.item_addr = vp.addr 
                                     WHERE master_addr = %s""", (instr.master_addr,)).fetchall()

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
        'new_tool_calls': tool_calls
    })

    return tool_calls
