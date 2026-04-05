-- notifiers
CREATE OR REPLACE FUNCTION count_updates()
    RETURNS TRIGGER AS $$
    BEGIN
        IF nextval('update_counter_window') IS 1000 THEN
            PERFORM pg_notify('window_recreate', 'TRUE'::TEXT);
            PERMORM setval('update_counter_window', 0);
        END IF;

    RETURN NEW;
END;
$$, LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_knowledge_update
AFTER UPDATE ON knowledge
FOR EACH ROW EXECUTE FUNCTION increment_update_count();

CREATE OR REPLACE TRIGGER trg_executable_update
AFTER UPDATE ON executables
FOR EACH ROW EXECUTE FUNCTION increment_update_count();
