-- notifiers
-- CREATE OR REPLACE FUNCTION count_updates()
--     RETURNS TRIGGER AS $$
--     BEGIN
--         IF nextval('update_counter_window') IS 1000 THEN
--             PERFORM pg_notify('window_recreate', 'TRUE'::TEXT);
--             PERMORM setval('update_counter_window', 0);
--         END IF;
-- 
--     RETURN NEW;
-- END;
-- $$, LANGUAGE plpgsql;
-- 
-- CREATE OR REPLACE TRIGGER trg_knowledge_update
-- AFTER UPDATE ON knowledge
-- FOR EACH ROW EXECUTE FUNCTION increment_update_count();
-- 
-- CREATE OR REPLACE TRIGGER trg_executable_update
-- AFTER UPDATE ON executables
-- FOR EACH ROW EXECUTE FUNCTION increment_update_count();

CREATE OR REPLACE FUNCTION position_calculation()
    RETURNS TRIGGER AS $$
    DECLARE
        item_1_pos NUMERIC;
        item_2_pos NUMERIC;
        item_1_pos_k NUMERIC;
        item_2_pos_k NUMERIC;
        item_1_distance DOUBLE PRECISION;
        item_2_distance DOUBLE PRECISION;
        item_1_distance_k DOUBLE PRECISION;
        item_2_distance_k DOUBLE PRECISION;
    BEGIN 
        SELECT position, NEW.emb <=> e.emb AS distance INTO item_1_pos, item_1_distance FROM executables e ORDER BY NEW.emb <=> e.emb LIMIT 1;
        SELECT position, NEW.emb <=> k.emb AS distance INTO item_1_pos_k, item_1_distance_k FROM knowledge k ORDER BY NEW.emb <=> k.emb LIMIT 1;

        SELECT position, NEW.emb <=> e.emb AS distance INTO item_2_pos, item_2_distance FROM executables e ORDER BY NEW.emb <=> e.emb OFFSET 1 LIMIT 1;
        SELECT position, NEW.emb <=> k.emb AS distance INTO item_2_pos_k, item_2_distance_k FROM knowledge k ORDER BY NEW.emb <=> k.emb OFFSET 1 LIMIT 1;

        IF item_1_distance_k < item_1_distance THEN
            item_1_distance := item_1_distance_k
            item_1_pos := item_1_pos_k
        END IF;

        IF item_2_distance_k < item_2_distance THEN
            item_2_distance := item_2_distance_k
            item_2_pos := item_2_pos_k
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
