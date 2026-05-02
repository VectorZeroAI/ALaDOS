INSERT INTO names(addr, name) VALUES 
(%s, 
    (SELECT 'session_'||(COALESCE(MAX(regexp_replace(name, '^session_', '')::int), 0) + 1)
        FROM names WHERE name ~ '^session_\d+$')
