CREATE OR REPLACE FUNCTION position_placeholder()
    RETURNS TRIGGER AS $$
    DECLARE
        max_pos NUMERIC;
    BEGIN
        NEW.position := nextval('vector_ops_position');
    RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER pos_placeholder
BEFORE INSERT ON vector_ops
FOR EACH ROW EXECUTE FUNCTION position_placeholder();

CREATE OR REPLACE FUNCTION position_calculation()
    RETURNS TRIGGER AS $$
    DECLARE
        item_1_pos NUMERIC;
        item_2_pos NUMERIC;
        item_1_distance DOUBLE PRECISION;
        item_2_distance DOUBLE PRECISION;
    BEGIN 
        SELECT position, NEW.emb <=> emb AS distance INTO item_1_pos, item_1_distance
        FROM vector_ops WHERE emb IS NOT NULL AND addr != NEW.addr ORDER BY distance LIMIT 1;

        SELECT position, NEW.emb <=> emb AS distance INTO item_2_pos, item_2_distance
        FROM vector_ops WHERE emb IS NOT NULL AND addr != NEW.addr ORDER BY distance LIMIT 1 OFFSET 1;

        IF item_1_pos IS NULL THEN
            NEW.position := 0;
            RETURN NEW;
        END IF;

        IF item_2_pos IS NULL THEN
            NEW.position := 1;
            RETURN NEW;
        END IF;

        IF item_1_distance > 0.4 THEN
            NEW.position := COALESCE((SELECT MAX(position) FROM vector_ops), 0) + 100;
            RETURN NEW;
        END IF;

        IF item_2_distance > 0.4 THEN
            NEW.position := (COALESCE((SELECT position FROM vector_ops WHERE position > item_1_pos ORDER BY position LIMIT 1), item_1_pos) + item_1_pos) / 2;
            RETURN NEW;
        END IF;
        
        NEW.position := (item_1_pos + item_2_pos) / 2;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER pos_calculate
BEFORE UPDATE OF emb ON vector_ops
FOR EACH ROW EXECUTE FUNCTION position_calculation(); 

CREATE OR REPLACE FUNCTION master_decomposition_slave_submission()
RETURNS TRIGGER AS $$
    DECLARE
        v_name TEXT;
    BEGIN
        SELECT name INTO v_name FROM names WHERE addr = NEW.addr;

        IF v_name LIKE 'session_%' THEN
            RETURN NEW;
        END IF;

        IF v_name LIKE '_rmt_activation%' THEN
            RETURN NEW;
        END IF;

        PERFORM new_slave(
            p_master_addr := NEW.addr, 
            p_instruction := ' Your task is to create a plan for the following instruction OR IF the task is simple, directly write a result via result.add_master_result tool. Master instruction: "
            ' || NEW.instruction || '" 
            You can add slaves via "goal.add_slave" tool. Slaves are steps in the plan, which is called master. A master instruction quallifies as simple if you can directly write a full answer to it with the tools you currently have, and it does not require any planing or steps at all. (plan MUST end with a planner slave UNLESS its done).
            YOU MUST OUTPUT A JSON ARRAY OF TOOL CALLS!!! DO NOT TRY TO MAKE THE ENTIRE PLAN AT ONCE, leave it to be incrementally produced via further planner slaves.',
            p_name := 'planner_'||nextval('global_planner_serial')::TEXT,
            p_slave_scope := 'task'
        );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER master_decompose_task
AFTER INSERT ON masters
FOR EACH ROW EXECUTE FUNCTION master_decomposition_slave_submission();


CREATE OR REPLACE FUNCTION init_master_context()
RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO master_context(addr, master_result, window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l)
        VALUES(NEW.addr, '', NULL, NULL, NULL, NULL);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER init_master_context
AFTER INSERT ON masters
FOR EACH ROW EXECUTE FUNCTION init_master_context();


CREATE OR REPLACE FUNCTION init_master_result()
RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO results(metadata) VALUES(jsonb_build_object(
            'type', 'master'
        )) RETURNING addr INTO NEW.result_addr;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER init_master_result_trg
BEFORE INSERT ON masters
FOR EACH ROW EXECUTE FUNCTION init_master_result();


CREATE OR REPLACE FUNCTION notify_for_ai_msg()
RETURNS TRIGGER AS $$
    BEGIN
        IF (NEW.metadata->>'type' = 'ai_message') AND (NEW.ready = TRUE) THEN
            EXECUTE format('NOTIFY %I, %L',
                NEW.metadata ->> 'session_name',
                'ai_message'||NEW.content_str::TEXT);
        END IF;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER notify_for_ai_msg
AFTER UPDATE ON results
FOR EACH ROW EXECUTE FUNCTION notify_for_ai_msg();


CREATE OR REPLACE FUNCTION check_for_master_completion()
RETURNS TRIGGER AS $$
    DECLARE
        v_master_addr BIGINT;
    BEGIN
        IF OLD.ready = NEW.ready OR NEW.ready = FALSE THEN
            RETURN NEW;
        END IF;

        IF EXISTS (SELECT 1 FROM masters WHERE result_addr = NEW.addr) THEN
            RETURN NEW;
        END IF;

        v_master_addr := (SELECT master_addr FROM slaves WHERE result_addr = NEW.addr);

        IF v_master_addr IS NULL THEN
            RETURN NEW; -- its not an result of a slave, disregard.
        END IF;

        IF NOT EXISTS (
            SELECT 1
            FROM slaves s
                INNER JOIN results r ON r.addr = s.result_addr
            WHERE s.master_addr = v_master_addr
                AND r.ready = FALSE
        ) THEN
            PERFORM new_result(
                p_content := (SELECT mc.master_result
                    FROM results r
                        JOIN slaves s ON s.result_addr = r.addr
                        JOIN master_context mc ON mc.addr = s.master_addr
                    WHERE mc.addr = v_master_addr),
                p_addr := (SELECT m.result_addr
                    FROM results r
                        JOIN slaves s ON s.result_addr = r.addr
                        JOIN masters m ON m.addr = s.master_addr
                    WHERE m.addr = v_master_addr)
            );
        END IF;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER check_master_completion_trg
AFTER UPDATE ON results
FOR EACH ROW EXECUTE FUNCTION check_for_master_completion();


CREATE OR REPLACE FUNCTION notify_cronjob_changes()
RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('cronjob_changes', TRUE::TEXT);
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER notify_cronjob_once_deleted
AFTER DELETE ON cronjob_once
FOR EACH ROW EXECUTE FUNCTION notify_cronjob_changes();

CREATE OR REPLACE TRIGGER notify_cronjob_loop_deleted
AFTER DELETE ON cronjob_loop
FOR EACH ROW EXECUTE FUNCTION notify_cronjob_changes();

CREATE OR REPLACE TRIGGER notify_cronjob_once_added
AFTER INSERT ON cronjob_once
FOR EACH ROW EXECUTE FUNCTION notify_cronjob_changes();

CREATE OR REPLACE TRIGGER notify_cronjob_loop_added
AFTER INSERT ON cronjob_loop
FOR EACH ROW EXECUTE FUNCTION notify_cronjob_changes();

