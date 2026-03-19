-- notifiers

CREATE OR REPLACE FUNCTION notify_result_inserted()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('new_result', NEW.addr::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_result
AFTER INSERT ON results
FOR EACH ROW EXECUTE FUNCTION notify_result_inserted();

CREATE OR REPLACE FUNCTION notify_context_changed()
    RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('context', JOIN NEW.addr AND NEW.window_position AND NEW.window_size_r AND NEW.window_site_l);  -- FIXME: Make the join syntax actually working
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_context
AFTER UPDATE ON master_context
FOR EACH ROW EXECUTE FUNCTION notify_context_changed();
