CREATE OR REPLACE FUNCTION position_placeholder()
    RETURNS TRIGGER AS $$
    DECLARE
        max_pos NUMERIC;
    BEGIN
        SELECT MAX(position) INTO max_pos FROM viewing_window;
        NEW.position := COALESCE(max_pos, 0) + 100;
    RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER pos_placeholder_e
BEFORE INSERT ON executables
FOR EACH ROW EXECUTE FUNCTION position_placeholder();

CREATE OR REPLACE TRIGGER pos_placeholder_k
BEFORE INSERT ON knowledge
FOR EACH ROW EXECUTE FUNCTION position_placeholder();

CREATE OR REPLACE FUNCTION position_calculation()
    RETURNS TRIGGER AS $$
    DECLARE
        item_1_pos NUMERIC;
        item_2_pos NUMERIC;
        item_1_distance DOUBLE PRECISION;
        item_2_distance DOUBLE PRECISION;
        item_1_pos_e NUMERIC;
        item_2_pos_e NUMERIC;
        item_1_distance_e DOUBLE PRECISION;
        item_2_distance_e DOUBLE PRECISION;
    BEGIN 
        SELECT position, NEW.emb <=> emb AS distance INTO item_1_pos, item_1_distance
        FROM knowledge WHERE emb IS NOT NULL AND emb != NEW.emb ORDER BY distance LIMIT 1;

        SELECT position, NEW.emb <=> emb AS distance INTO item_1_pos_e, item_1_distance_e
        FROM executables WHERE emb IS NOT NULL AND emb != NEW.emb ORDER BY distance LIMIT 1;
        
        SELECT position, NEW.emb <=> emb AS distance INTO item_2_pos, item_2_distance
        FROM knowledge WHERE emb IS NOT NULL AND emb != NEW.emb ORDER BY distance LIMIT 1 OFFSET 1;

        SELECT position, NEW.emb <=> emb AS distance INTO item_2_pos_e, item_2_distance_e
        FROM executables WHERE emb IS NOT NULL AND emb != NEW.emb ORDER BY distance LIMIT 1 OFFSET 1;

        IF item_1_distance_e < item_1_distance THEN
            item_1_distance := item_1_distance_e;
            item_1_pos := item_1_pos_e;
        END IF;

        IF item_2_distance_e < item_2_distance THEN
            item_2_distance := item_2_distance_e;
            item_2_pos := item_2_pos_e;
        END IF;

        IF item_1_pos IS NULL THEN
            NEW.position := 0;
            RETURN NEW;
        END IF;

        IF item_2_pos IS NULL THEN
            NEW.position := 1;
            RETURN NEW;
        END IF;

        IF item_1_distance > 0.4 THEN
            NEW.position := COALESCE((SELECT MAX(position) FROM viewing_window), 0) + 100;
            RETURN NEW;
        END IF;

        IF item_2_distance > 0.4 THEN
            NEW.position := (COALESCE((SELECT position FROM viewing_window WHERE position > item_1_pos ORDER BY position LIMIT 1), item_1_pos) + item_1_pos) / 2;
            RETURN NEW;
        END IF;
        
        NEW.position := (item_1_pos + item_2_pos) / 2;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER pos_calculate_k
BEFORE UPDATE OF emb ON knowledge
FOR EACH ROW EXECUTE FUNCTION position_calculation(); 

CREATE OR REPLACE TRIGGER pos_calculate_e
BEFORE UPDATE OF emb ON executables
FOR EACH ROW EXECUTE FUNCTION position_calculation();

CREATE OR REPLACE FUNCTION master_decomposition_slave_submission()
RETURNS TRIGGER AS $$
    DECLARE
        v_name TEXT;
    BEGIN
        SELECT name INTO v_name FROM names WHERE addr = NEW.addr;

        IF name LIKE 'session_%' THEN
            RETURN NEW;
        END IF;

        PERFORM new_slave(NEW.addr, 
            ' Your task is to create a plan for the following instruction OR IF the task is simple, directly write a result via result.add_master_result tool. Master instruction: "
            ' || NEW.instruction || '" 
            You can add slaves via "goal.add_slave" tool. Slaves are steps in the plan, which is called master. A master instruction quallifies as simple if you can directly write a full answer to it with the tools you currently have, and it does not require any planing or steps at all. (plan MUST end with a planner slave UNLESS its done).
            YOU MUST OUTPUT A JSON ARRAY OF TOOL CALLS!!! DO NOT TRY TO MAKE THE ENTIRE PLAN AT ONCE, leave it to be incrementally produced via further planner slaves.',
            NULL, NULL, NULL, NULL, NULL, 'task'
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
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER init_master_context
AFTER INSERT ON masters
FOR EACH ROW EXECUTE FUNCTION init_master_context();


CREATE OR REPLACE FUNCTION notify_for_ai_msg()
RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.metadata->>'type' = 'ai_message' THEN
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


CREATE OR REPLACE FUNCTION notify_cronjob_changes()
RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('cronjob_changes', TRUE);
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
