CREATE EXTENSION IF NOT EXISTS vector;

CREATE SEQUENCE IF NOT EXISTS global_next_id;

CREATE TABLE IF NOT EXISTS addrs (
    addr BIGINT DEFAULT nextval('global_next_id') PRIMARY KEY,
);

CREATE TABLE IF NOT EXISTS names(
    name TEXT PRIMARY KEY,
    addr BIGINT REFERENCES addrs(addr) ON DELETE CASCADE
)

CREATE TABLE IF NOT EXISTS knowledge (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    content TEXT NOT NULL,
    desc TEXT NOT NULL, -- used for semantic similarity search
    embedding vector() -- TODO: ADD DIMENSIONS
);

CREATE TABLE IF NOT EXISTS executables (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    desc TEXT NOT NULL, -- used for semantic similarity search
    header TEXT NOT NULL, -- the usage manual (imperative)
    body TEXT NOT NULL,
    head_emb vector() -- TODO: ADD DIMENSIONS
);

CREATE TABLE IF NOT EXISTS logs (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    action TEXT NOT NULL,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()))::BIGINT,
    created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS masters (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    window_position INT,
    window_size_r INT,
    window_size_l INT,
    CONSTRAINT CHECK ( 
        (window_position IS NULL AND window_size_l IS NULL and window_size_r IS NULL) 
        OR 
        (window_position IS NOT NULL AND window_size_l IS NOT NULL and window_size_r IS NOT NULL)
    )
);

CREATE TABLE master_load(
    master_addr BIGINT REFERENCES masters(addr),
    item_addr BIGINT REFERENCES addrs(addr),
    PRIMARY KEY (master_addr, item_addr)
);

CREATE TABLE IF NOT EXISTS results (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    content_json JSONB,
    content_str TEXT
);

CREATE TABLE IF NOT EXISTS slaves (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    master_addr BIGINT REFERENCES masters(addr),
    instruction TEXT NOT NULL,
    results_addr BIGINT REFERENCES results(addr),
    result_name TEXT
);

CREATE TABLE IF NOT EXISTS slave_req (
    slave_addr BIGINT REFERENCES slaves(addr) ON DELETE CASCADE,
    req_addr BIGINT REFERENCES addrs(addr) ON DELETE CASCADE,
PRIMARY KEY (slave_addr, req_addr)
);

CREATE TABLE IF NOT EXISTS ownership(
    addr BIGINT REFERENCES addrs(addr) ON DELETE CASCADE PRIMARY KEY,
    owner BIGINT REFERENCES masters(addr) NOT NULL
);

CREATE OR REPLACE FUNCTION new_addr() RETURNS BIGINT AS $$
    DECLARE
        new_id BIGINT;
    BEGIN
        INSERT INTO addrs DEFAULT VALUES RETURNING addr INTO new_id;
    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

-- new slave function definition

CREATE OR REPLACE FUNCTION new_slave(
    p_master_addr BIGINT,
    p_name TEXT,
    p_instruction TEXT,
    p_requires BIGINT[],
    p_results_addr BIGINT DEFAULT NULL,
    p_result_name TEXT DEFAULT NULL
    )

    RETURNS BIGINT AS $$
    DECLARE
        new_slave_addr BIGINT;
        req BIGINT;
    BEGIN
        INSERT INTO slaves (master_addr, name, instruction, results_addr, result_name)
        VALUES (p_master_addr, p_name, p_instruction, p_results_addr, p_result_name)
    RETURNING addr INTO new_slave_addr;

    FOREACH req IN ARRAY p_requires LOOP
        INSERT INTO slave_req (slave_addr, req_addr) VALUES (new_slave_addr, req);
    END LOOP;

    RETURN new_slave_addr;
END;
$$ LANGUAGE plpgsql;

-- Notifier for the sceduler
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
        PERFORM pg_notify('context', NEW.);   
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_context
AFTER UPDATE ON masters
FOR EACH ROW EXECUTE FUNCTION notify_context_changed();

CREATE OR REPLACE FUNCTION resolve_name(p_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_addr BIGINT;
BEGIN
    SELECT addr INTO v_addr FROM addr_names WHERE name = p_name;

    IF v_addr IS NULL THEN
        RAISE EXCEPTION 'Unknown name: %', p_name;
    END IF;

    RETURN v_addr;
END;
$$ LANGUAGE plpgsql;
