
-- The SQL querry performed on startup.
-- Performs a full scan of the DB for slaves that are unblocked, and returns all the addrs of them
SELECT s.addr FROM slaves s
WHERE NOT EXISTS (
    SELECT 1 FROM slave_req sr
    JOIN results r ON sr.req_addr = r.addr
    WHERE sr.slave_addr = s.addr
    AND r.ready IS FALSE
)
