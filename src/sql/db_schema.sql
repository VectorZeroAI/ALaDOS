CREATE EXTENSION IF NOT EXISTS vector;

CREATE SEQUENCE IF NOT EXISTS global_next_id;

CREATE OR REPLACE FUNCTION new_addr() RETURNS BIGINT AS $$
    DECLARE
        new_id BIGINT;
    BEGIN
        INSERT INTO addrs DEFAULT VALUES RETURNING addr INTO new_id;
    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS addrs (
    addr BIGINT DEFAULT nextval('global_next_id') PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS names(
    name TEXT PRIMARY KEY,
    addr BIGINT REFERENCES addrs(addr) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS knowledge (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    content TEXT NOT NULL,
    desc TEXT NOT NULL,
    embedding vector(384)
);

CREATE TABLE IF NOT EXISTS executables (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    desc TEXT NOT NULL, -- used for semantic similarity search
    header TEXT NOT NULL, -- the usage manual (imperative)
    body TEXT NOT NULL,
    emb vector(384)
);

CREATE TABLE IF NOT EXISTS logs (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE,
    action TEXT NOT NULL,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()))::BIGINT,
    created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS masters (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY REFERENCES addrs(addr) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS master_context (
    addr BIGINT PRIMARY KEY REFERENCES masters(addr) ON DELETE CASCADE,
    window_position INT,
    window_size_r INT,
    window_size_l INT,
    CONSTRAINT window_full_or_none CHECK ( 
        (window_position IS NULL AND window_size_l IS NULL and window_size_r IS NULL)
        OR
        (window_position IS NOT NULL AND window_size_l IS NOT NULL and window_size_r IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS master_load (
    master_addr BIGINT REFERENCES masters(addr) ON DELETE CASCADE,
    item_addr BIGINT REFERENCES addrs(addr) ON DELETE CASCADE,
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

