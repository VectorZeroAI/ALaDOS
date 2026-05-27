CREATE OR REPLACE FUNCTION create_session(
    p_first_msg TEXT,
    p_ai_prompt TEXT
)
RETURNS TEXT AS $$
DECLARE
    v_session_addr BIGINT;
    v_session_name TEXT;
    v_usr_msg_addr BIGINT;
    v_usr_msg_placeholder_addr BIGINT;
BEGIN
    v_session_addr := new_master(instruction:= "You will be answering human messages. There is no need to plan anything.");

    INSERT INTO names(addr, name) VALUES(
        v_session_addr, 'session_'||(SELECT COALESCE(MAX(
                (regexp_replace(name, 'session_', '')::int) + 1 
            ), 0) FROM names WHERE name LIKE 'session_%')::TEXT
    ) RETURNING name INTO v_session_name;
    
    INSERT INTO results (metadata) VALUES (
        jsonb_build_object('type', 'human_message',
            'turn', 1,
            'session_name', v_session_name
    )) RETURNING addr INTO v_usr_msg_addr;

    PERFORM new_slave(
        p_master_addr := v_session_addr,
        p_instruction := p_ai_prompt,
        p_requires := ARRAY[v_usr_msg_addr],
        p_slave_scope := '_webui'
    );

    INSERT INTO results(metadata)
    VALUES (jsonb_build_object('type', 'ai_message',
                    'turn', 1,
                    'session_name', v_session_name
    ));


    PERFORM new_result('User message: "'||p_first_msg||' "', v_usr_msg_addr);
    
    INSERT INTO results(metadata) VALUES (
        jsonb_build_object(
            'type', 'human_message', 
            'session_name', v_session_name,
            'turn', 2
        )
    ) RETURNING addr INTO v_usr_msg_placeholder_addr;
    
    
    PERFORM new_slave(
        p_master_addr := v_session_addr,
        p_instruction := p_ai_prompt,
        p_requires := ARRAY[v_usr_msg_placeholder_addr],
        p_slave_scope := '_webui'
    );

    INSERT INTO results(metadata)
    VALUES (jsonb_build_object('type', 'ai_message',
                    'turn', 2,
                    'session_name', v_session_name
    ));
    
    RETURN v_session_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_messages(
    session_name TEXT
)
RETURNS table(turn INT, human_msg TEXT, ai_msg TEXT) AS $$
DECLARE
    v_session_addr BIGINT;
BEGIN
    v_session_addr := resolve_name(session_name);

    RETURN QUERY
    WITH hm AS(
        SELECT content_str AS message, (metadata->>'turn')::int AS turn
        FROM results
        WHERE ready = TRUE
            AND metadata->>'type' = 'human_message'
            AND metadata->>'session_name' = session_name
        ORDER BY turn ASC
    ), am AS (
        SELECT content_str as message, (metadata ->> 'turn')::int AS turn
        FROM results
        WHERE ready = TRUE
            AND metadata->>'type' = 'ai_message'
            AND metadata->>'session_name' = session_name
        ORDER BY turn ASC
    )
    SELECT hm.turn as turn,
        hm.message as human_msg,
        COALESCE(am.message, 'ai did not answer yet') as ai_msg
    FROM hm
    LEFT JOIN am USING (turn)
    ORDER BY hm.turn;

END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION submit_human_msg(
    msg_text TEXT,
    session_name TEXT,
    ai_instruction TEXT
)
RETURNS VOID AS $$
DECLARE
    session_addr BIGINT;
    human_msg_destination_addr BIGINT;
    next_result_addr BIGINT;
    next_result_turn INT;
BEGIN
    session_addr := resolve_name(session_name);

    SELECT addr FROM results
    WHERE metadata->>'type' = 'human_message'
        AND metadata->>'session_name' = session_name
        AND ready = FALSE
    ORDER BY (metadata->>'turn')::int
    LIMIT 1 INTO human_msg_destination_addr;

    PERFORM new_result(msg_text, human_msg_destination_addr);

    SELECT MAX((metadata ->> 'turn')::int) + 1
    FROM results
    WHERE metadata @> jsonb_build_object('type', 'human_message', 'session_name', session_name)
    INTO next_result_turn;

    INSERT INTO results(metadata) 
    VALUES (jsonb_build_object('type', 'human_message',
            'session_name', session_name,
            'turn', next_result_turn))
    RETURNING addr INTO next_result_addr;

    PERFORM new_slave(
        p_master_addr := session_addr,
        p_instruction := ai_instruction,
        p_requires := ARRAY[next_result_addr],
        p_slave_scope := '_webui'
    );

    INSERT INTO results(metadata)
    VALUES (jsonb_build_object('type', 'ai_message',
                    'turn', next_result_turn,
                    'session_name', session_name
    ));

    RETURN;

END;
$$ LANGUAGE plpgsql;
