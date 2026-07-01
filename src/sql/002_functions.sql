CREATE OR REPLACE FUNCTION new_slave(
    p_master_addr BIGINT,
    p_instruction TEXT,
    p_name TEXT DEFAULT NULL,
    p_requires BIGINT[] DEFAULT NULL,
    p_result_addr BIGINT DEFAULT NULL,
    p_result_name TEXT DEFAULT NULL,
    p_result_metadata JSONB DEFAULT NULL,
    p_slave_scope slave_scope DEFAULT 'general'
    )

    RETURNS BIGINT AS $$
    DECLARE
        new_slave_addr BIGINT;
        req BIGINT;
        v_result_addr BIGINT;
        flag_any_result_not_ready BOOLEAN;
    BEGIN
        flag_any_result_not_ready := FALSE;
        new_slave_addr := new_addr();
        v_result_addr := COALESCE(p_result_addr, new_addr());

        IF p_result_addr IS NULL THEN
            INSERT INTO results (addr, metadata) VALUES (v_result_addr, p_result_metadata);
        END IF;

        IF p_result_name IS NOT NULL THEN
            INSERT INTO names (addr, name) VALUES (v_result_addr, p_result_name);
        END IF;

        INSERT INTO slaves (master_addr, instruction, result_addr, addr, scope)
        VALUES (p_master_addr, p_instruction, v_result_addr, new_slave_addr, p_slave_scope);
        
        IF p_name IS NOT NULL THEN
            INSERT INTO names (addr, name) VALUES (new_slave_addr, p_name);
        END IF;

        IF p_requires IS NULL THEN
            PERFORM pg_notify('slaves_ready', new_slave_addr::TEXT);
            RETURN new_slave_addr;
        ELSE
            FOREACH req IN ARRAY p_requires LOOP
                INSERT INTO slave_req (slave_addr, req_addr) VALUES (new_slave_addr, req);
                IF (SELECT ready FROM results WHERE addr = req) IS FALSE THEN
                    flag_any_result_not_ready := TRUE;
                END IF;
            END LOOP;
        END IF;
        IF flag_any_result_not_ready IS FALSE THEN
            PERFORM pg_notify('slaves_ready', new_slave_addr::TEXT);
        END IF;

    RETURN new_slave_addr;
END;
$$ LANGUAGE plpgsql;

-- resolves name
CREATE OR REPLACE FUNCTION resolve_name(p_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_addr BIGINT;
BEGIN
    SELECT addr INTO v_addr FROM names WHERE name = p_name;

    IF v_addr IS NULL THEN
        RAISE EXCEPTION 'Unknown name: %', p_name;
    END IF;

    RETURN v_addr;
END;
$$ LANGUAGE plpgsql;


-- new result function
CREATE OR REPLACE FUNCTION new_result(
    p_content TEXT,
    p_addr BIGINT DEFAULT NULL,
    p_name TEXT DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE 
    unblocked BIGINT;
    v_addr BIGINT;
BEGIN
    -- resolve results addr from result name or from 
    IF p_addr IS NOT NULL THEN
        v_addr := p_addr;
    ELSIF p_name IS NOT NULL THEN
        SELECT resolve_name(p_name) INTO v_addr;
    ELSE
        RAISE EXCEPTION 'one of p_addr or p_name is required. None were given.';
    END IF;

    -- Mark result ready
    UPDATE results SET ready = TRUE, content_str = p_content, status = NULL, status_inf = NULL WHERE addr = v_addr;

    -- Find and notify newly unblocked slaves
    FOR unblocked IN
        SELECT s.addr FROM slaves s
            LEFT JOIN slave_req sr ON sr.slave_addr = s.addr
            JOIN masters m ON m.addr = s.master_addr
            LEFT JOIN master_req mr ON mr.master_addr = s.master_addr
            LEFT JOIN results r_m ON r_m.addr = mr.req_addr
        WHERE sr.req_addr = v_addr
            AND NOT EXISTS (
                SELECT 1
                FROM slave_req sr2
                    JOIN results r ON r.addr = sr2.req_addr
                WHERE sr2.slave_addr = s.addr
                    AND r.ready = FALSE
                )
            AND (r_m.ready IS NULL OR r_m.ready = TRUE)
    LOOP
        PERFORM pg_notify('slaves_ready', unblocked::TEXT);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION s_land (
    master_addr_p BIGINT,
    emb_p vector(768)
)
RETURNS VOID AS $$
DECLARE
    result_addr BIGINT;
    max_sim FLOAT;
    tttt TEXT;
BEGIN
    SELECT addr, 1 - (emb_p <=> emb) as similarity, type INTO result_addr, max_sim, tttt FROM vector_ops ORDER BY similarity DESC LIMIT 1;
    
    IF result_addr IS NULL THEN
        RAISE exception'No item to anchor on found.';
        RETURN;
    END IF;
    
    IF tttt = 'knowledge' THEN
        UPDATE master_context SET
            window_anchor_knowledge = result_addr,
            window_anchor_exe = NULL,
            window_size_l = 12,
            window_size_r = 12
        WHERE addr = master_addr_p;
    END IF;
    IF tttt = 'executable' THEN
        UPDATE master_context SET
            window_anchor_exe = result_addr,
            window_anchor_knowledge = NULL,
            window_size_l = 12,
            window_size_r = 12
        WHERE addr = master_addr_p;
    END IF;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION move_anchor(
    p_amount INT,
    p_master_id BIGINT
)
RETURNS VOID AS $$
DECLARE
    v_anchor_addr BIGINT;
    v_new_addr BIGINT;
    v_new_type TEXT;
BEGIN
    SELECT COALESCE(
        (SELECT window_anchor_knowledge FROM master_context WHERE addr = p_master_id), 
        (SELECT window_anchor_exe FROM master_context WHERE addr = p_master_id)
    ) INTO v_anchor_addr;
    
    WITH ordered AS (
        SELECT description,
            addr,
            position,
            type,
            ROW_NUMBER() OVER (ORDER BY position) AS rn FROM vector_ops
    ), anchor AS (
        SELECT rn FROM ordered WHERE addr = v_anchor_addr LIMIT 1
    )
    SELECT o.addr, o.type INTO v_new_addr, v_new_type
    FROM ordered o, anchor a
    WHERE o.rn = a.rn + p_amount;

    IF v_new_type = 'knowledge' THEN
        UPDATE master_context SET
            window_anchor_knowledge = v_new_addr,
            window_anchor_exe = NULL
        WHERE addr = p_master_id;
    ELSIF v_new_type = 'executable' THEN
        UPDATE master_context SET
            window_anchor_exe = v_new_addr,
            window_anchor_knowledge = NULL
        WHERE addr = p_master_id;
    ELSIF v_new_type IS NULL THEN
        RAISE EXCEPTION'no rows to move to.';
    ELSE 
        RAISE EXCEPTION'unexpected type of anchor. Type: %, expected "knowledge" or "executable"', v_new_type;
    END IF;

    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION save_rmt(
    p_parsed_rmt JSONB,
    p_name TEXT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    slaves_table rmt_slaves%ROWTYPE[];
    single_slave rmt_slaves%ROWTYPE;
    temporary_table inter_repr[];
    tmp_el inter_repr;
    v_template_addr BIGINT;
    step rmt_node;
    deps_addrs BIGINT[];
    v_parsed_rmt rmt_node[];
BEGIN

    INSERT INTO reusable_master_templates DEFAULT VALUES RETURNING addr INTO v_template_addr;

    IF p_name IS NOT NULL THEN
        INSERT INTO names(addr, name) VALUES (v_template_addr, p_name);
    END IF;

    SELECT array_agg(j) INTO v_parsed_rmt
    FROM jsonb_populate_recordset(NULL::rmt_node, p_parsed_rmt) AS j;

    FOREACH step IN ARRAY v_parsed_rmt LOOP
        
        tmp_el := NULL::inter_repr;
        tmp_el.instruction := step.instruction;
        tmp_el.id := step.id;
        tmp_el.scope := step.scope;
        tmp_el.deps_txt := step.deps;
        tmp_el.addr = new_addr();

        temporary_table := array_append(temporary_table, tmp_el);

    END LOOP;

    FOR i IN 1..array_length(temporary_table, 1) LOOP

        SELECT array_agg(addr) FROM unnest(temporary_table) WHERE id = ANY(temporary_table[i].deps_txt) AND addr != temporary_table[i].addr INTO deps_addrs;

        temporary_table[i].deps_addr := deps_addrs;

    END LOOP;

    single_slave.master_addr := NULL;

    FOREACH tmp_el IN ARRAY temporary_table LOOP

        single_slave.addr := tmp_el.addr;
        single_slave.instruction := tmp_el.instruction;
        single_slave.result_addr := new_addr();
        single_slave.scope := tmp_el.scope;
        single_slave.deps := tmp_el.deps_addr;
        single_slave.template_addr := v_template_addr;

        slaves_table := array_append(slaves_table, single_slave);

    END LOOP;

    INSERT INTO rmt_slaves SELECT (unnest(slaves_table)).*;

    RETURN v_template_addr;
    
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION new_master(
    p_instruction TEXT,
    req_names TEXT[] DEFAULT NULL,
    req_addrs BIGINT[] DEFAULT NULL,
    result_name TEXT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    new_master_addr BIGINT;
    new_master_result_addr BIGINT;
    v_addr BIGINT;
    name TEXT;
BEGIN

    INSERT INTO masters(instruction) VALUES (p_instruction)
    RETURNING addr, result_addr
    INTO new_master_addr, new_master_result_addr;
    
    IF req_names IS NOT NULL THEN
        FOREACH name IN ARRAY req_names LOOP
            req_addrs := req_addrs || resolve_name(name);
        END LOOP;
    END IF;
    
    IF req_addrs IS NOT NULL THEN
        FOREACH v_addr IN ARRAY req_addrs LOOP
            INSERT INTO master_req(master_addr, req_addr) VALUES(new_master_addr, addr);
        END LOOP;
    END IF;

    IF result_name IS NOT NULL THEN
        INSERT INTO names(addr, name) VALUES(new_master_result_addr, result_name);
    END IF;

    RETURN new_master_addr;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION activate_rmt_as_master(
    p_rmt_addr BIGINT,
    p_depends_on BIGINT[],
    p_required_by BIGINT[],
    p_inputs JSONB
) RETURNS VOID AS $$

master_addr_querry = plpy.prepare(""" SELECT new_master(
    p_instruction := 'NONE',
    req_addrs := $1
) """, ["BIGINT[]"])

master_addr = plpy.execute(master_addr_querry, [p_depends_on,])[0][0]

master_result_addr_querry = plpy.prepare("""
SELECT result_addr FROM masters WHERE addr = $1;
""", ["BIGINT"])

master_result_addr = plpy.execute(master_result_addr_querry, [master_addr,])[0][0]

insert_into_slave_req_querry = plpy.prepare("""
        INSERT INTO slave_req(slave_addr, req_addr) VALUES ($1, $2)
        """, ["BIGINT", "BIGINT"])

for i in p_required_by:
    insert_into_slave_req_querry.execute([i, master_result_addr])
    
select_rmt_template_querry = plpy.prepare("""
SELECT addr,
    master_addr,
    instruction,
    result_addr,
    scope, deps
FROM rmt_slaves
WHERE template_addr = $1
""", ["BIGINT"])

rmt_template = select_rmt_template_querry.execute([p_rmt_addr,])

"""
And now its time to translate the deps. So, I need to update the entire thing.
I will do it naively, cause the thing is,
    if performance of this thing will be bad enough to care,
    I will rewrite into plpgsql, and get some speed there.
As long as this is not the case, I will just continue with the O(n * n) approach.
"""

for i in rmt_template:
    old_addr = i.result_addr

    i.addr = plpy.execute("SELECT new_addr()")
    i.result_addr = plpy.execute("SELECT new_addr()")

    for j in rmt_template:
        for indx, k in enumerate(j.deps):
            if k == old_addr:
                j.deps[indx] = i.result_addr

new_slave_querry = plpy.prepare("""SELECT new_slave(
        p_master_addr := %s
        p_instruction := %s
        p_requires := %s
        p_result_addr := %s
        p_slave_scope := %s
    )""", ["BIGINT", "TEXT", "BIGINT[]", "BIGINT", "slave_scope"])


for i in rmt_template:
    new_slave_querry.execute([
        master_result_addr,
        i.instruction,
        i.deps,
        i.result_addr,
        i.scope
    ]
)

$$ LANGUAGE plpython3u SECURITY DEFINER;
