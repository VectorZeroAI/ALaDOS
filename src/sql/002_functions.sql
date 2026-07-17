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

    FOREACH tmp_el IN ARRAY temporary_table LOOP

        single_slave.addr := tmp_el.addr;
        single_slave.instruction := tmp_el.instruction;
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
    result_name TEXT DEFAULT NULL,
    p_addr BIGINT DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    new_master_addr BIGINT;
    new_master_result_addr BIGINT;
    v_addr BIGINT;
    name TEXT;
BEGIN

    new_master_addr := COALESCE(p_addr, new_addr());

    INSERT INTO masters(instruction, addr) VALUES (p_instruction, new_master_addr)
    RETURNING result_addr
    INTO new_master_result_addr;
    
    IF req_names IS NOT NULL THEN
        FOREACH name IN ARRAY req_names LOOP
            req_addrs := req_addrs || resolve_name(name);
        END LOOP;
    END IF;
    
    IF req_addrs IS NOT NULL THEN
        FOREACH v_addr IN ARRAY req_addrs LOOP
            INSERT INTO master_req(master_addr, req_addr) VALUES(new_master_addr, v_addr);
        END LOOP;
    END IF;

    IF result_name IS NOT NULL THEN
        INSERT INTO names(addr, name) VALUES(new_master_result_addr, result_name);
    END IF;

    RETURN new_master_addr;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION recursive_walk_forwards_slaves_dag(
    p_start_node BIGINT
) RETURNS BIGINT[] AS $$
DECLARE
    v_nodes_list BIGINT[];
BEGIN
    WITH RECURSIVE forward_walk(nodes) AS (
        -- ANCHOR

        SELECT p_start_node

        UNION ALL

        SELECT next_addr
        FROM forward_walk fw
        JOIN slaves s ON s.addr=fw.nodes
        LEFT JOIN LATERAL (
            -- LEG 1: 
            -- Base case, just go forward through the graph.

            SELECT slave_addr AS next_addr
            FROM slave_req sr
            WHERE sr.req_addr = s.result_addr

            UNION ALL
            
            -- LEG 2:
            -- Case 2: result required by master
            -- goes through master, of course only if master really required the result,
            -- and just goes through to the first nodes of the master.

            SELECT s2.addr AS next_addr
            FROM master_req mr
                JOIN slaves s2 ON s2.master_addr = mr.master_addr
            WHERE mr.req_addr = s.result_addr
                AND NOT EXISTS (
                    SELECT 1
                    FROM slave_req
                    WHERE slave_addr = s2.addr
                )

            UNION ALL
            
            -- LEG 3:
            -- Case 3: No more slaves, in the master, try to jump through master result
            -- Checks if no more slaves present in the graph, and if yes,
            -- then it goes to masters result, and then checks through master result, and looks if anyone requires it.
            -- if yes, for slave case, it just gives the slave, and for master case,
            -- it goes into the master and selects the starting slaves of the master, and returns them,
            -- All in a single querry. 

            SELECT COALESCE(sr.slave_addr, s2.addr) AS next_addr
            FROM masters m
                LEFT JOIN slave_req sr ON m.result_addr = sr.req_addr
                LEFT JOIN master_req mr ON m.result_addr = mr.req_addr
                JOIN slaves s2 ON mr.master_addr = s2.master_addr
            WHERE m.addr = s.master_addr
                AND NOT EXISTS (
                    SELECT 1 FROM slave_req WHERE req_addr = s.result_addr
                ) AND NOT EXISTS (
                    SELECT 1
                    FROM slave_req sr2
                        JOIN slaves s3 ON s3.addr = sr2.slave_addr
                )
        ) legs ON TRUE
    ) SELECT array_agg(nodes) INTO v_nodes_list FROM forward_walk;

    RETURN v_nodes_list;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION recursive_walk_backwards_slaves_dag(
    p_start_node BIGINT
) RETURNS BIGINT[] AS $$
DECLARE
    v_nodes_list BIGINT[];
BEGIN
    WITH RECURSIVE backward_walk(nodes) AS (
        -- ANCHOR
        SELECT p_start_node

        UNION ALL


        SELECT next_node
        FROM backward_walk
            JOIN slaves s_b ON s_b.addr = backward_walk.nodes
            JOIN slave_req sr_b ON sr_b.slave_addr = backward_walk.nodes
        LEFT JOIN LATERAL (
            -- RECURSE, uses backward_walk.nodes as those are the previous results
            -- just gets the requirements of a node.
            SELECT s.addr AS next_node
            FROM slaves s
            WHERE s.result_addr = sr_b.req_addr

            UNION ALL

            -- LEG 2, EDGE CASE "req_addr is a master_result"
            -- Only executed for those that arent slave results.
            -- Goes to the master, and grabs its slaves.
            -- Grabs only the slaves of the master that are not required anywhere,
            -- for it to then naturally continue downwards

            SELECT s1.addr AS next_node
            FROM masters m
                INNER JOIN slaves s1 ON s1.master_addr = m.addr
            WHERE NOT EXISTS(SELECT 1 FROM slave_req sr WHERE sr.req_addr = s1.result_addr)
                AND m.result_addr = sr_b.req_addr

            UNION ALL

            -- LEG 3, the edge case of "No requirements left of slaves"
            -- Executed only for slaves that have no requirements.
            -- Has 2 cases inside of it melded together for performance.
            -- sub case 1: master_req is a master_result
            -- sub case 2: master_req is a slave_result
            -- in both cases it just left joins the crap in, that means it keeps the result address
            -- while getting new info.
            -- In sub case 2 this leg just does the same unraveling as LEG 2 does.
            -- COALESCE because only one of those is true for one row, e.g. for one cause, e.g. for one result. 
            -- The only unhandled case is if a result came out of the fucking sky, e.g. from external sources,
            -- And that would be ignored, because thats what its supposed to do.
            -- NOTE : All required results with unclear origin are ignored. 

            SELECT COALESCE(s2.addr, s3.addr) AS next_node
            FROM master_req mr
                LEFT JOIN slaves s2 ON mr.req_addr = s2.result_addr
                LEFT JOIN masters m ON mr.req_addr = m.result_addr
                JOIN slaves s3 ON s3.master_addr = m.addr
            WHERE COALESCE(s2.addr, s3.addr) IS NOT NULL
                AND NOT EXISTS(SELECT 1 FROM slave_req sr WHERE sr.req_addr = s3.result_addr)
                AND mr.master_addr = s_b.master_addr
        ) legs ON TRUE

    ) SELECT array_agg(nodes) INTO v_nodes_list FROM backward_walk;

    RETURN v_nodes_list;

END;
$$ LANGUAGE plpgsql;
