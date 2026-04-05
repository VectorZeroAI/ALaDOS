-- notifiers

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

CREATE OR REPLACE FUNCTION count_updates()
    RETURNS TRIGGER AS $$
    BEGIN
        IF nextval('update_counter_window') IS 1000 THEN
            PERFORM pg_notify('window', 'TRUE'::TEXT);
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
