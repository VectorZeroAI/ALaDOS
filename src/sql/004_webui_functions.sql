CREATE OR REPLACE FUNCTION create_session(
    p_first_msg TEXT,
    p_ai_prompt TEXT
)
RETURNS BIGINT AS $$
DECLARE
    v_session_addr BIGINT;
    v_session_name TEXT;
    v_usr_msg_addr BIGINT;
    v_usr_msg_placeholder_addr BIGINT;
BEGIN
    INSERT INTO masters(instruction)
    VALUES('YOU WILL BE ANSWERING HUMAN MESSAGES.
        THERE IS NO NEED TO PLAN ANYTHING. JUST OUTPUT OKAY AND THATS IT.')
    RETURNING addr INTO v_session_addr;
    
    INSERT INTO names(addr, name)
    VALUES (v_session_addr, 
        (SELECT 'session_'||(COALESCE(MAX(regexp_replace(name, '^session_', '')::int), 0) + 1)::text 
            FROM names WHERE name ~ '^session_\d+$') RETURNING name INTO v_session_name);

    INSERT INTO results DEFAULT VALUES RETURNING addr INTO v_usr_msg_addr;
    INSERT INTO names(addr, name) VALUES (v_usr_msg_addr, 'human_message_1')

    PERFORM new_slave(v_session_addr, p_ai_prompt, NULL, ARRAY(v_usr_msg_addr), NULL, 'ai_message_1');

    PERFORM new_result('User message: "'||p_first_msg||' "', v_usr_msg_addr);
    
    INSERT INTO results DEFAULT VALUES RETURNING addr INTO v_usr_placeholder_addr;
    INSERT INTO names(addr, name) VALUES(v_usr_placeholder_addr,
        'human_message_'||(SELECT MAX(regexp_replace(name, '^session_', '')::int) + 1)::text
        FROM names 
        WHERE name LIKE 'session\_%' ESCAPE '\')
    
    PERFORM new_slave(v_session_addr, p_ai_prompt, NULL, ARRAY(v_usr_placeholder_addr), NULL,
        'ai_message_'||(SELECT MAX(regexp_replace(name, '^ai_message_', '')::int) + 1 
        FROM names WHERE name LIKE 'ai_message_%')::text)
    
    RETURN v_session_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_messages(
    session_name TEXT
)
RETURNS table(human_msg TEXT, ai_msg TEXT, turn INT) AS $$
DECLARE
    v_session_addr BIGINT;
BEGIN
    v_session_addr := resolve_name(session_name);

    RETURN QUERRY
    WITH hm AS(
        SELECT r.content_str as message,
            regexp_replace(name, '^human_message_', '')::int AS turn
        FROM results r
            INNER JOIN names n ON n.addr = r.addr
            INNER JOIN slave_req sr ON sr.req_addr = r.addr
            INNER JOIN slaves s ON s.addr = sr.slave_addr
        WHERE n.name ~ 'human\_message\_%' ESCAPE '\'
            AND s.master_addr = v_session_addr
            AND r.ready = TRUE
        ORDER BY turn ASC
    ), WITH am AS (
        SELECT r.content_str as message,
            regexp_replace(name, '^ai_message_', '')::int AS turn
        FROM results r
            INNER JOIN names n ON n.addr = r.addr
            INNER JOIN slave_req sr ON sr.req_addr = r.addr
            INNER JOIN slaves s ON s.addr = sr.slave_addr
        WHERE n.name ~ 'ai\_message\_%' ESCAPE '\'
            AND s.master_addr = v_session_addr
            AND r.ready = TRUE
        ORDER BY turn ASC
    )
    SELECT hm.turn as turn,
        hm.message as human_msg,
        COALESCE(a.ai, 'ai did not answer yet') as ai_msg
    FROM hm
    LEFT JOIN am USING (turn)
    ORDER BY hm.turn;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION submit_human_msg(
    msg_text TEXT,
    session_name TEXT
)
RETURNS VOID AS $$
DECLARE
    v_session_addr BIGINT;
    human_msg_destination_addr BIGINT;
    next_result_addr BIGINT;
BEGIN
    v_session_addr := resolve_name(msg_text);

    SELECT n.addr, regexp_replace(name, 'human_message_', '')::INT as turn
    FROM names n
        INNER JOIN results r ON r.addr = n.addr
        INNER JOIN slave_req sr ON sr.req_addr = r.addr
        INNER JOIN slaves s ON sr.slave_addr = s.addr
    WHERE name ~ 'human\_message\_%' ESCAPE '\'
        AND r.ready = FALSE
        AND s.master_addr = v_session_addr
    ORDER BY turn ASC
    LIMIT 1 INTO human_msg_destination_addr;

    PERFORM new_result(msg_text, human_msg_desctination_addr);

    INSERT INTO results DEFAULT VALUES RETURNING addr INTO next_result_addr;
    INSERT INTO names(addr, name) VALUES (next_result_addr, 
        'human_message_'||(SELECT regexp_replace(name, 'human_message_', '')::int + 1 
            FROM names WHERE name ~ 'human\_message\_%' ESCAPE '\')
    )

    PERFORM new_slave
    
    
    
    

$$ LANGUAGE plpgsql;
