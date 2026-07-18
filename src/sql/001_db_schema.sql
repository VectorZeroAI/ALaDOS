CREATE EXTENSION IF NOT EXISTS vector;

CREATE SEQUENCE IF NOT EXISTS global_next_id;

CREATE SEQUENCE IF NOT EXISTS global_planner_serial;

CREATE SEQUENCE IF NOT EXISTS global_rmt_activation_serial;

CREATE SEQUENCE IF NOT EXISTS vector_ops_position
    START WITH 100
    INCREMENT BY 100;

DO $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_type WHERE typname = 'slave_scope') THEN
        CREATE TYPE slave_scope AS ENUM('all', 'general', 'context', 'task', 'communication', '_webui', '_rmt');
    END IF;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_type WHERE typname = 'rmt_node') THEN
        CREATE TYPE rmt_node AS (
            instruction TEXT,
            id TEXT,
            deps TEXT[],
            scope slave_scope
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'inter_repr') THEN
        CREATE TYPE inter_repr AS (
            instruction TEXT,
            id TEXT,
            addr BIGINT,
            deps_txt TEXT[],
            deps_addr BIGINT[],
            scope slave_scope
        );
    END IF;
END;
$$ LANGUAGE plpgsql;





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
    content TEXT NOT NULL -- NOTE : Names, aka titles, are always stored in names table
);

CREATE TABLE IF NOT EXISTS executables (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    header TEXT NOT NULL, -- the usage manual (imperative)
    body TEXT NOT NULL -- NOTE : Names, aka titles, are always stored in names table
);

CREATE TABLE IF NOT EXISTS vector_ops(
    addr_exe BIGINT REFERENCES executables(addr) ON UPDATE CASCADE ON DELETE CASCADE,
    addr_k BIGINT REFERENCES knowledge(addr) ON UPDATE CASCADE ON DELETE CASCADE,
    addr BIGINT GENERATED ALWAYS AS (COALESCE(addr_exe, addr_k)) STORED,
    position NUMERIC UNIQUE NOT NULL,
    description TEXT NOT NULL,
    version INT DEFAULT 0,
    type TEXT GENERATED ALWAYS AS (CASE
        WHEN addr_exe IS NOT NULL THEN 'executable'
        WHEN addr_k IS NOT NULL THEN 'knowledge'
    END) VIRTUAL,
    emb vector(768),
    CONSTRAINT addr_or_exe_ref_not_both_not_none CHECK (NOT
        ((addr_exe IS NOT NULL AND addr_k IS NOT NULL)
        OR
        (addr_exe IS NULL AND addr_k IS NULL))
    ),
    PRIMARY KEY (addr)
);


CREATE TABLE IF NOT EXISTS logs (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW()))::BIGINT,
    content JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    content_str TEXT,
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT, -- Status, e.g. error, paradox, impossible instruction.
    status_inf JSONB, -- additional unstructured information, with per status different keys and values.
    metadata JSONB, -- for things such as type for webui sessions, and other crap
    CONSTRAINT content_present_when_ready CHECK (
        (ready IS FALSE AND content_str IS NULL)
        OR 
        (ready IS TRUE AND content_str IS NOT NULL)
    ),
    CONSTRAINT status_inf_not_without_status CHECK (
        NOT(status_inf IS NOT NULL AND status IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS masters (
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr)
            ON DELETE CASCADE 
            ON UPDATE CASCADE,
    instruction TEXT NOT NULL,
    result_addr BIGINT NOT NULL
        REFERENCES results(addr)
            ON DELETE RESTRICT
            ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS master_context (
    addr BIGINT PRIMARY KEY 
        REFERENCES masters(addr)
            ON DELETE CASCADE 
            ON UPDATE CASCADE,
    master_result TEXT NOT NULL,
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
    result_addr BIGINT UNIQUE
        REFERENCES results(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    scope slave_scope NOT NULL DEFAULT 'general'
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

CREATE TABLE IF NOT EXISTS master_req (
    master_addr BIGINT
        REFERENCES masters(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    req_addr BIGINT
        REFERENCES results(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    PRIMARY KEY (master_addr, req_addr)
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

CREATE TABLE IF NOT EXISTS cronjob_once(
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    body TEXT NOT NULL,
    args JSONB NOT NULL,
    start_after INTEGER NOT NULL, -- unix epoch
    finished BOOLEAN NOT NULL DEFAULT FALSE,
    error BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    CONSTRAINT error_text_not_without_error CHECK (
        (error IS TRUE AND error_text IS NOT NULL)
        OR
        (error IS TRUE AND error_text IS NULL)
        OR
        (error is FALSE AND error_text IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS cronjob_loop(
    addr BIGINT DEFAULT new_addr() PRIMARY KEY
        REFERENCES addrs(addr)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    body TEXT NOT NULL,
    args JSONB NOT NULL,
    execute_every INTEGER NOT NULL, -- seconds
    last_ran INTEGER NOT NULL DEFAULT 0, -- unix epoch
    error BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    CONSTRAINT error_text_not_without_error CHECK (
        (error IS TRUE AND error_text IS NOT NULL)
        OR
        (error IS TRUE AND error_text IS NULL)
        OR
        (error is FALSE AND error_text IS NULL)
    )

);

CREATE OR REPLACE VIEW cronjobs_to_run AS 
    SELECT addr, body, start_after AS run_at, 'cronjob_once' AS type, args AS params FROM cronjob_once WHERE finished = FALSE AND error = FALSE
    UNION ALL
    SELECT addr, body, (last_ran + execute_every) as run_at, 'cronjob_loop' AS type, args AS params FROM cronjob_loop WHERE error = FALSE
    ORDER BY run_at ASC;



CREATE TABLE IF NOT EXISTS reusable_master_templates(
    addr BIGINT PRIMARY KEY DEFAULT new_addr() REFERENCES addrs(addr)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS rmt_slaves(
    addr BIGINT DEFAULT new_addr() PRIMARY KEY 
        REFERENCES addrs(addr) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
    instruction TEXT NOT NULL,
    scope slave_scope NOT NULL DEFAULT 'general',
    template_addr BIGINT
        REFERENCES reusable_master_templates
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    deps BIGINT[]
);
