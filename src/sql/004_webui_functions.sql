CREATE OR REPLACE FUNCTION create_session(
    p_first_msg TEXT,
    p_ai_prompt TEXT
)
RETURNS BIGINT AS $$
DECLARE
    v_session_addr BIGINT;
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
            FROM names WHERE name ~ '^session_\d+$') RETURNING name);

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
    
    RETURN v_session_addr;
END;
$$ LANGUAGE plpgsql;
