CREATE OR REPLACE FUNCTION new_slave(
    p_master_addr BIGINT,
    p_name TEXT,
    p_instruction TEXT,
    p_requires BIGINT[],
    p_results_addr BIGINT DEFAULT NULL,
    p_result_name TEXT DEFAULT NULL
    )

    RETURNS BIGINT AS $$
    DECLARE
        new_slave_addr BIGINT;
        req BIGINT;
    BEGIN
        INSERT INTO slaves (master_addr, name, instruction, results_addr, result_name)
        VALUES (p_master_addr, p_name, p_instruction, p_results_addr, p_result_name)
    RETURNING addr INTO new_slave_addr;

    FOREACH req IN ARRAY p_requires LOOP
        INSERT INTO slave_req (slave_addr, req_addr) VALUES (new_slave_addr, req);
    END LOOP;

    RETURN new_slave_addr;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION resolve_name(p_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_addr BIGINT;
BEGIN
    SELECT addr INTO v_addr FROM addr_names WHERE name = p_name;

    IF v_addr IS NULL THEN
        RAISE EXCEPTION 'Unknown name: %', p_name;
    END IF;

    RETURN v_addr;
END;
$$ LANGUAGE plpgsql;
