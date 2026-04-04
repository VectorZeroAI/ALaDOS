-- notifiers

CREATE OR REPLACE FUNCTION new_result(
    p_content TEXT,
    p_addr BIGINT DEFAULT NULL,
    p_name TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE 
    unblocked BIGINT
    v_addr BIGINT
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
    UPDATE results SET ready = TRUE, content_str = p_content
    WHERE addr = v_addr

    -- Find and notify newly unblocked slaves
    FOR unblocked IN
        SELECT s.addr FROM slaves s
        JOIN slave_req sr ON sr.slave_addr = s.addr
        WHERE sr.req_addr = p_slave_addr
        AND NOT EXISTS (
            SELECT 1 FROM slave_req sr2
            JOIN slaves dep ON dep.addr = sr2.req_addr
            JOIN results r ON r.addr = dep.results_addr
            WHERE sr2.slave_addr = s.addr AND r.ready = FALSE
        )
    LOOP
        PERFORM pg_notify('slaves_ready', unblocked::TEXT);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION notify_result_inserted()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('new_result', NEW.addr::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_notify_result
AFTER INSERT ON results
FOR EACH ROW EXECUTE FUNCTION notify_result_inserted();

CREATE OR REPLACE FUNCTION notify_context_changed()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify(
            'context',
            json_build_object(
                'addr', NEW.addr,
                'window_position', NEW.window_position,
                'window_size_l', NEW.window_size_l,
                'window_size_r', NEW.window_size_r
            )::TEXT
        );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_notify_context
AFTER UPDATE ON master_context
FOR EACH ROW EXECUTE FUNCTION notify_context_changed();
