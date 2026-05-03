#!/usr/bin/env python3
"""
This is the file where the whole web UI sever will be. 
Architecture is simple: 
    endpoint for the web app
    endpoint for fetching events
    websocket for new events. 
    
"""
from typing import Literal, TypeAlias
from flask import Flask, request, jsonify
from ..utils.conn_factory import conn_factory
import psycopg

ai_msg: TypeAlias = str
h_msg: TypeAlias = str


AI_SESSION_HANDLER_PROMPT = 'Your task is to be a helffull assistant, to truthfully answer users questions, and to execute users instructions via tools. You must also answer the user. To answer the user, use the tool result.write, DO NOT JUST ANSWER PLAINTEXT.'


webserver = Flask(__name__)

@webserver.route('/', methods=['GET', 'POST'])
def root_webpage():
    return 'MAIN PAGE! Go to /chat to chat.', 200

@webserver.route('/chat/<session_id>', methods=['POST'])
def chat_page(session_id: int):
    return 'PLACEHOLDER PAGE, to be filled in with HTML CSS AND JS later. Also has to somehow get the session.', 200


@webserver.route('/_/create_session', methods=['POST'])
def create_session():
    data = request.get_json()
    conn = conn_factory()

    if not data or 'first_msg' not in data:
        return jsonify({'success': False, 'reason': 'first_msg is missing from the json payload'}), 400

    session_name = _create_session_sql(data['first_msg'], conn)

    return jsonify({
        'success': True, 'session_name': session_name
    }), 201


@webserver.route("/_/load_session", methods=['POST'])
def load_session(session_name: str):
    conn = conn_factory()

    messages_array = _get_messages(session_name, conn)
    
    return jsonify(messages_array), 200

@webserver.route("/_/submit_user_message", methods=["POST"])
def submit_user_message():
    data = request.get_json()
    if not data or any('user_message', 'session_name') not in data:
        return jsonify({'success': False, 'reason': 'provided json was invalid.', 'provided_json': data}), 500

    


def _create_session_sql(first_msg: str, conn: psycopg.Connection):
    session_name = conn.execute("""
    SELECT create_session(%s, %s)
                 """, (first_msg, AI_SESSION_HANDLER_PROMPT))

    return session_name






def _get_messages(session_name: str, conn: psycopg.Connection):

    messages_array = conn.execute("""
    SELECT turn, human_msg, ai_msg FROM get_messages(%s);
                 """, (session_name)).fetchall()
    
    return messages_array



def _submit_human_message(msg: str, session_name: str, conn: psycopg.Connection):

    
    addr_next_result = conn.execute(r"""
SELECT n.addr
FROM names n 
    INNER JOIN results r ON n.addr = r.addr 
    INNER JOIN slave_req sr ON r.addr = sr.req_addr
    INNER JOIN slaves s ON sr.slave_addr = s.addr
WHERE name LIKE 'human\_message\_%' ESCAPE '\'
    AND r.ready = FALSE
    AND s.master_addr = %s
ORDER BY regexp_replace(n.name, '^human_message_', '')::int DESC
LIMIT 1;
                                        """, (session_addr,)).fetchone()[0]

    conn.execute("""
SELECT new_result(%s, %s);
                 """, (msg, addr_next_result))

    new_human_msg_addr = conn.execute("""
INSERT INTO results DEFAULT VALUES RETURNING addr;
                 """)
    
    conn.execute(r"""
INSERT INTO names(addr, name) VALUES(%s, 'human_message_'||(SELECT MAX(regexp_replace(name, '^human_message_', '')::int) FROM names WHERE name LIKE 'human\_message\_%')::text)
                 """, (new_human_msg_addr,))
    
    conn.execute("""
SELECT new_slave(%s, %s, NULL, %s, NULL, 'ai_message_'||(SELECT MAX(regexp_replace(name, 'ai_message'))));
     """, (
         session_addr,
         AI_SESSION_HANDLER_PROMPT,
         [new_human_msg_addr]
     ))




"""
The graph structure of messages in a session is the following:
    result with name "human_message_{int}" --> slave with name "ai_action_{int}", on the same int = the user message and its response. 
    So we have to iterate over the ints from max to bottom and grab those tuples, wich are r.content_str and turn id. 
    So basically the 2 results with the same turn id, e.g. the the number at the end, are of the same turn. 
    So we can just grab every tunrs human messages and ai_actions, via the ~ or LIKE comparasons, and then turn them into an array in python.

"""



"""
So, the sessions are as following: they are masters with name session_{session_name}.
They have 1 result, that is not ready, auto created, as a placeholeder for human message,
    that is set to ready and human message: contents into content_str
They have a slave with instruction of "answer the human AND execute appropriate tools,
    Write your answer as text into result.write"
"""
