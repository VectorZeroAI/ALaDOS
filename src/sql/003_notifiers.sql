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

        IF item_1_pos IS NULL AND item_1_distance IS NULL THEN
            NEW.position := nextval('vector_ops_position');
            RETURN NEW;
        END IF;

        IF item_2_pos IS NULL AND item_2_distance IS NULL THEN
            NEW.position := nextval('vector_ops_position');
            RETURN NEW;
        END IF;

        IF item_1_distance > 0.4 THEN
            NEW.position := nextval('vector_ops_position');
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
            You can add slaves via "goal.add_slave" tool.
            You can add masters via "goal.add_master" tool.
            Slaves are atomic self contained signle Reason + Act steps in the plan.
            A master instruction quallifies as simple if you can directly write a full answer to it with the tools you currently have,
            and it does not require any planing or steps at all.
            (plan segment MUST end with a planner slave UNLESS its done, if you made the whole plan then the plan is done.).

            You have 2 available strategies for planning: strategy 1 - Recurse, strategy 2 - Incrementality. 

            Strategy 2 - Incrementality: 
                You decompose the master instruction into slaves (atomic self contained Reason + Act operations) incrementally,
                end each incremental step with a planner slave,
                and thus completing the task via a bunch of plan segments for incrementality. 
                Incrementality is done to prevent the results diverging from expected outcome, which happens with static long plans, wich is why this strategy requires incremental generation.
            Strategy 1 - Recurse:
                You split the task into major steps, and declare them as masters,
                and build the complete plan out of masters.
                Masters can achive complex tasks, while Slaves are only for atomic operations. 

            When to choose wich strategy:
            Pick strategy 2 if the task can be broken down into atomic ReAct steps directly with high certanty of correctness and low hurdle. 
            Pick stragegy 1 if the task cant be broken down into atomic ReAct steps directly, or it is highly complex or just broad and big.

            If the task is trivial, just directly give the result. Result of the master_instruction must end up in master_result.
            ',
            p_name := 'planner_'||nextval('global_planner_serial')::TEXT,
            --- TODO : Add a step to this,
            --- where there available rmts are evaluated on the propability of them being usefull 
            --- to the task, and then embedded into the prompt as possibilities of executing functions
            --- directly insdead of planning stuff.
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
        v_content TEXT;
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

            SELECT mc.master_result INTO v_content FROM master_context mc WHERE mc.addr = v_master_addr;

            SELECT v_content||COALESCE(string_agg(r.content_str, ''), '\n') INTO v_content
            FROM slave_req sr
                RIGHT JOIN slaves s ON sr.slave_addr = s.addr
                JOIN results r ON s.result_addr = r.addr
            WHERE NOT EXISTS (
                    SELECT 1 FROM slave_req WHERE req_addr = r.addr
                )
                AND s.master_addr = v_master_addr
                AND r.content_str NOT LIKE '%Added a master result.%';
            -- NOTE : This querry checks the contents of the result for having wrote to master_result,
            -- and if no, concatenates to v_content. 

            PERFORM new_result(
                p_content := v_content,
                p_addr :=  (SELECT m.result_addr FROM masters m WHERE m.addr = v_master_addr)
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

