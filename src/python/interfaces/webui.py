#!/usr/bin/env python3
"""
This is the file where the whole web UI sever will be. 
Architecture is simple: 
    endpoint for the web app
    endpoint for fetching events
    websocket for new events. 
    
"""
from flask import Flask, request, jsonify
from ..utils.conn_factory import conn_factory
import psycopg

webserver = Flask(__name__)
conn = conn_factory()

@webserver.route('/', methods=['GET', 'POST'])
def root_webpage():
    return 'MAIN PAGE! Go to /chat to chat.', 200

@webserver.route('/chat', methods=['POST'])
def chat_page():
    return 'PLACEHOLDER PAGE, to be filled in with HTML CSS AND JS later', 200


@webserver.route('/_/create_session', methods=['POST'])
def create_session():
    data = request.get_json()

    if not data or 'first_msg' not in data:
        return jsonify({'success': False, 'reason': 'first_msg is missing from the json payload'}), 400

    session_id = _create_session_sql(data['first_msg'])

    return jsonify({
        'success': True, 'session_id': session_id
    }), 201
    


def _create_session_sql(first_msg: str):
    master_addr = conn.execute(r"""
    INSERT INTO masters(instruction) VALUES('Be a good assistant to the user. Do what the user says.') RETURNING addr;
                 """).fetchone()[0]

    session_name = conn.execute(r"""
    INSERT INTO names(addr, name) VALUES (%s, (SELECT 'session_'||(COALESCE(MAX(regexp_replace(name, '^session_', '')::int), 0) + 1) FROM names WHERE name ~ '^session_\d+$')
                                """, (master_addr, )).fetchone()[0]

    result_addr = conn.execute(r"""
    INSERT INTO results DEFAULT VALUES RETURNING addr;
                 """, (first_msg,)).fetchone()[0]
    conn.execute(r"""
    SELECT new_slave(%s, %s, NULL, %s)
    """, (master_addr,
          'Your task is to be a helffull assistant, to truthfully answer users questions, and to execute users instructions via tools. You must also answer the user. To answer the user, use the tool result.write, DO NOT JUST ANSWER PLAINTEXT.',
          result_addr))
    conn.execute(r"""
    SELECT new_result(%s, %s);
                 """, (f"User message: '{first_msg}'", result_addr))
    return session_name

    



















"""
So, the sessions are as following: they are masters with name session_{session_name}.
They have 1 result, that is not ready, auto created, as a placeholeder for human message,
    that is set to ready and human message: contents into content_str
They have a slave with instruction of "answer the human AND execute appropriate tools,
    Write your answer as text into result.write"
"""
