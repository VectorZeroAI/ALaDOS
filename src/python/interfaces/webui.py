#!/usr/bin/env python3
"""
This is the file where the whole web UI sever will be. 
Architecture is simple: 
    endpoint for the web app
    endpoint for fetching events
    websocket for new events. 
    
"""
from typing import TypeAlias
from flask import Flask, Response, render_template_string, request, jsonify
from psycopg import sql
from ..utils.conn_factory import conn_factory
import psycopg
from pathlib import Path

ai_msg: TypeAlias = str
h_msg: TypeAlias = str


AI_SESSION_HANDLER_PROMPT = 'Your task is to be a helffull assistant, to truthfully answer users questions, and to execute users instructions via tools. You must also answer the user. DO NOT JUST ANSWER PLAINTEXT.'


webserver = Flask(__name__)

@webserver.route('/', methods=['GET', 'POST'])
def root_webpage():
    with open(Path(__file__).resolve().parent / 'webuipages' / 'mainpage.html') as f:
        return render_template_string(f.read())

@webserver.route('/chat/<session_name>', methods=['POST', 'GET'])
def chat_page(session_name: str):
    with open(Path(__file__).resolve().parent / 'webuipages' / 'chatpage.html') as f:
        return render_template_string(f.read(), session_name=session_name)


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


@webserver.route("/_/load_session/<session_name>", methods=['GET', 'POST'])
def load_session(session_name: str):
    conn = conn_factory()

    messages_array = _get_messages(session_name, conn)
    
    return jsonify(messages_array), 200


@webserver.route("/_/submit_user_message", methods=["POST"])
def submit_user_message():
    conn = conn_factory()

    data = request.get_json()

    if not data or 'user_message' not in data or 'session_name' not in data:
        return jsonify({'success': False, 'reason': 'provided json was invalid.', 'provided_json': data}), 500

    _submit_human_message(data['user_message'], data['session_name'], conn)

    return jsonify({'success': True}), 200


@webserver.route("/_/list_session_names_and_ids", methods=["GET", "POST"])
def list_session_names_and_ids():
    conn = conn_factory()
    session_names_and_ids = _get_session_names_and_ids(conn)
    return jsonify(session_names_and_ids), 200


@webserver.route('/_/ai_msg_stream/<session_name>', methods=['GET'])
def stream_ai_responses(session_name: str):
    conn = conn_factory()

    def ai_messages_yielder():
        conn.execute(
            sql.SQL("LISTEN {}").format(sql.Identifier(session_name,)) # NOTE : Dynamic SQL shenanigan
        )
        for n in conn.notifies():
            if not n.channel == session_name:
                continue
            yield n.payload

    return Response(
        ai_messages_yielder(),
        mimetype='text/event-stream',
        headers={"Cache-Control": 'no-cache', "X-Accel-Buffering": 'no'}
    )


def _get_session_names_and_ids(conn: psycopg.Connection):
    return conn.execute("""
SELECT n.name, m.addr FROM masters m JOIN names n ON m.addr = n.addr WHERE n.name LIKE 'session_%'
                        """).fetchall()


def _create_session_sql(first_msg: str, conn: psycopg.Connection):
    session_name = conn.execute("""
    SELECT create_session(%s, %s);
                 """, (first_msg, AI_SESSION_HANDLER_PROMPT)).fetchone()[0]

    return session_name


def _get_messages(session_name: str, conn: psycopg.Connection):

    messages_array = conn.execute("""
    SELECT turn, human_msg, ai_msg FROM get_messages(%s);
                 """, (session_name, )).fetchall()
    
    return messages_array


def _submit_human_message(msg: str, session_name: str, conn: psycopg.Connection):

    conn.execute("""
    SELECT submit_human_msg(%s, %s, %s);
                 """, (msg, session_name, AI_SESSION_HANDLER_PROMPT))
    return





"""
The graph structure of messages in a session is the following:
    result with name "human_message_{int}" --> slave with name "ai_action_{int}", on the same int = the user message and its response. 
    So we have to iterate over the ints from max to bottom and grab those tuples, wich are r.content_str and turn id. 
    So basically the 2 results with the same turn id, e.g. the the number at the end, are of the same turn. 
    So we can just grab every tunrs human messages and ai_actions, via the ~ or LIKE comparasons, and then turn them into an array in python.

"""

"""
My lazy ass doesnt want to refactor these notes now, but just know that they may not be valid any longer,
because I added a metadata JSONB field to the results table, where a lot of the stuff that I used a ton of logic to infer just lives plaintext now.
"""
