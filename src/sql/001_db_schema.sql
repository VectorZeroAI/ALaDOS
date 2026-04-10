CREATE EXTENSION IF NOT EXISTS vector;

CREATE SEQUENCE IF NOT EXISTS global_next_id;

CREATE SEQUENCE IF NOT EXISTS update_counter_window;

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
    addr BIGINT UNIQUE
        REFERENCES addrs(addr)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS knowledge (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr)
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    content TEXT NOT NULL,
    description TEXT NOT NULL,
    position NUMERIC NOT NULL,
    emb vector(384) NOT NULL  -- NOTE : Names, aka titles, are always stored in names table
);

CREATE INDEX ON knowledge USING hnsw(emb vector_cosine_ops);

CREATE TABLE IF NOT EXISTS executables (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    description TEXT NOT NULL, -- used for semantic similarity search
    header TEXT NOT NULL, -- the usage manual (imperative)
    body TEXT NOT NULL,
    position NUMERIC NOT NULL,
    emb vector(384) NOT NULL -- NOTE : Names, aka titles, are always stored in names table
);

CREATE INDEX ON executables USING hnsw(emb vector_cosine_ops);

CREATE TABLE IF NOT EXISTS logs (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    action TEXT NOT NULL,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()))::BIGINT,
    created_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS masters (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr)
            ON DELETE CASCADE 
            ON UPDATE CASCADE,
    instruction TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS master_context (
    addr BIGINT PRIMARY KEY 
        REFERENCES masters(addr)
            ON DELETE CASCADE 
            ON UPDATE CASCADE,
    window_anchor_exe BIGINT
        REFERENCES executables(addr) 
            ON DELETE SET NULL 
            ON UPDATE CASCADE,
    window_anchor_knowledge BIGINT 
        REFERENCES knowledge(addr) 
            ON DELETE SET NULL 
            ON UPDATE CASCADE,
    window_size_r INT,
    window_size_l INT,
    CONSTRAINT window_full_or_none CHECK (
        (window_anchor_exe IS NULL AND window_anchor_knowledge IS NULL AND window_size_l IS NULL AND window_size_r IS NULL)
        OR
        (window_anchor_exe IS NOT NULL AND window_anchor_knowledge IS NULL AND window_size_l IS NOT NULL AND window_size_r IS NOT NULL)
        OR
        (window_anchor_exe IS NULL AND window_anchor_knowledge IS NOT NULL AND window_size_l IS NOT NULL AND window_size_r IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS master_load (
    master_addr BIGINT 
        REFERENCES masters(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    item_addr BIGINT 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    PRIMARY KEY (master_addr, item_addr)
);

CREATE TABLE IF NOT EXISTS results (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    content_str TEXT,
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT content_present_when_ready CHECK(
        (ready IS FALSE AND content_str IS NULL)
        OR 
        (ready IS TRUE AND content_str IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS slaves (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    master_addr BIGINT
        REFERENCES masters(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    instruction TEXT NOT NULL,
    result_addr BIGINT 
        REFERENCES results(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    result_name TEXT
);

CREATE TABLE IF NOT EXISTS slave_req (
    slave_addr BIGINT 
        REFERENCES slaves(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    req_addr BIGINT 
        REFERENCES results(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    PRIMARY KEY (slave_addr, req_addr)
);

CREATE TABLE IF NOT EXISTS ownership(
    addr BIGINT PRIMARY KEY 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    owner BIGINT NOT NULL 
        REFERENCES masters(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE
);

CREATE OR REPLACE VIEW viewing_window AS
    SELECT addr, description, emb, position, 'knowledge' AS type FROM knowledge
    UNION ALL
    SELECT addr, description, emb, position, 'executable' AS type FROM executables
    ORDER BY position;

CREATE OR REPLACE VIEW addrs_tables AS
    SELECT addr, 'knowledge' AS type FROM knowledge
    UNION ALL
    SELECT addr, 'executables' AS type FROM executables
    UNION ALL
    SELECT addr, 'logs' AS type FROM logs
    UNION ALL
    SELECT addr, 'masters' AS type FROM masters
    UNION ALL
    SELECT addr, 'slaves' AS type FROM slaves
    UNION ALL
    SELECT addr, 'results' AS type FROM results;
