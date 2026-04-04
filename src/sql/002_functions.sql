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
        INSERT INTO slaves (master_addr, instruction, results_addr, result_name)
        VALUES (p_master_addr, p_instruction, p_results_addr, p_result_name)
        RETURNING addr INTO new_slave_addr;

        INSERT INTO names (addr, name) VALUES (new_slave_addr, p_name);

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


-- new result function
CREATE OR REPLACE FUNCTION new_result(
    p_content TEXT,
    p_addr BIGINT DEFAULT NULL,
    p_name TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE 
    unblocked BIGINT;
    v_addr BIGINT;
BEGIN
    -- resolve results addr from result name or from 
    IF p_addr IS NOT NULL THEN
        v_addr := p_addr;
    ELSIF p_name IS NOT NULL THEN
        SELECT result_addr INTO v_addr FROM slaves WHERE result_name = p_name;
    ELSE
        RAISE EXCEPTION 'one of p_addr or p_name is required. None were given.';
    END IF;

    -- Mark result ready
    UPDATE results SET ready = TRUE, content_str = p_content WHERE addr = v_addr;

    -- Find and notify newly unblocked slaves
    FOR unblocked IN
        SELECT s.addr FROM slaves s
        JOIN slave_req sr ON sr.slave_addr = s.addr
        WHERE sr.req_addr = v_addr
        AND NOT EXISTS (
            SELECT 1 FROM slave_req sr2
            JOIN results r ON r.addr = sr2.req_addr
            WHERE sr2.slave_addr = s.addr AND r.ready = FALSE
        )
    LOOP
        PERFORM pg_notify('slaves_ready', unblocked::TEXT);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
