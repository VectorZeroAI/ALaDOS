CREATE OR REPLACE FUNCTION new_slave(
    p_master_addr BIGINT NOT NULL,
    p_name TEXT NOT NULL,
    p_instruction TEXT NOT NULL,
    p_requires BIGINT[] DEFAULT NULL,
    p_result_addr BIGINT DEFAULT NULL,
    p_result_name TEXT DEFAULT NULL
    )

    RETURNS BIGINT AS $$
    DECLARE
        new_slave_addr BIGINT;
        req BIGINT;
--         v_has_cycle BOOLEAN; NOTE : LATER MAYBE SOMEDAY
        v_result_addr BIGINT;
    BEGIN
        IF p_result_addr IS NULL THEN
            INSERT INTO addrs (addr) DEFAULT VALUES RETURNING addr INTO v_result_addr;
            INSERT INTO results (addr) VALUES (v_result_addr);

        END IF;

        IF p_result_name IS NOT NULL THEN
            INSERT INTO names (addr, name) VALUES ((COALESCE(p_result_addr, v_result_addr)), p_result_name);
        END IF;

        INSERT INTO slaves (master_addr, instruction, result_addr, result_name)
        VALUES (p_master_addr, p_instruction, p_result_addr, p_result_name)
        RETURNING addr INTO new_slave_addr;

        INSERT INTO names (addr, name) VALUES (new_slave_addr, p_name);

        IF p_requires IS NULL THEN
            PERFORM pg_notify('slaves_ready', new_slave_addr::TEXT);
            RETURN new_slave_addr;
        ELSE
            FOREACH req IN ARRAY p_requires LOOP
                INSERT INTO slave_req (slave_addr, req_addr) VALUES (new_slave_addr, req);
            END LOOP;
        END IF;


--         WITH RECURSIVE dep_chain(slave_addr) AS (
--             SELECT sr.slave_addr FROM slave_req sr WHERE sr.req_addr = COALESCE(p_result_addr, v_result_addr)
-- 
--             UNION NOTE : LATER MAYBE SOMEDAY, I cant figure that out. Any working version would be vibed, and propably not working.
-- 
--             SELECT sr.slave_addr FROM dep_chain dc
--             JOIN slaves s ON dc.slave_addr = s.slave_addr
--             JOIN slave_req sr ON sr.req_addr = s.result_addr
--         )
--         SELECT EXISTS (
--             SELECT 1 FROM dep_chain WHERE slave_addr = new_slave_addr
--         ) INTO v_has_cycle;
-- 
--         IF v_has_cycle THEN
--             RAISE EXCEPTION 'CYCLE DETECTED!!!';
--         END IF;

    RETURN new_slave_addr;
END;
$$ LANGUAGE plpgsql;

-- resolves name
CREATE OR REPLACE FUNCTION resolve_name(p_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_addr BIGINT;
BEGIN
    SELECT addr INTO v_addr FROM names WHERE name = p_name;

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
