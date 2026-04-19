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
        FROM knowledge WHERE emb IS NOT NULL ORDER BY distance LIMIT 1;

        SELECT position, NEW.emb <=> emb AS distance INTO item_1_pos_e, item_1_distance_e
        FROM executables WHERE emb IS NOT NULL ORDER BY distance LIMIT 1;
        
        SELECT position, NEW.emb <=> emb AS distance INTO item_2_pos, item_2_distance
        FROM knowledge WHERE emb IS NOT NULL ORDER BY distance LIMIT 1 OFFSET 1;

        SELECT position, NEW.emb <=> emb AS distance INTO item_2_pos_e, item_2_distance_e
        FROM executables WHERE emb IS NOT NULL ORDER BY distance LIMIT 1 OFFSET 1;

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
BEFORE INSERT ON knowledge
FOR EACH ROW EXECUTE FUNCTION position_calculation(); 

CREATE OR REPLACE TRIGGER pos_calculate_e
BEFORE INSERT ON executables
FOR EACH ROW EXECUTE FUNCTION position_calculation();

CREATE OR REPLACE FUNCTION master_decomposition_slave_submission()
RETURNS TRIGGER AS $$
    BEGIN
        PERFORM new_slave(NEW.addr, 
            'Your task is to decompose the following task into the initial steps, OR to directly execute the task, if the task is trivial.
            You can use add_slave tool to add slaves. '||NEW.instruction||' You should
            only provide the initial steps,
            and end the initial plan with an "create further plan steps" step,
            wich you must add via the "add_planner" tool. OR if the task is trivial, directly write a result to the master_result via the corresponding tool.'
        );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER master_decompose_task
AFTER INSERT ON masters
FOR EACH ROW EXECUTE FUNCTION master_decomposition_slave_submission();
