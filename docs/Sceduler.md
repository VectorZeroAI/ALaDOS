# Scheduler
The scheduler is a stateless event-driven component that determines which slave goals are ready for execution and pushes them to the executor queue.
## Trigger
The scheduler listens on the PostgreSQL new_result NOTIFY channel. Every time a result is inserted into the results table, the DB fires a notification containing the new result's addr. The scheduler wakes up only on these events — there is no polling.

## On each event
Query slave_req for all slaves that depend on the newly arrived result addr.
For each such slave, check if all its requirements now have corresponding entries in results.
If yes, push the slave addr to the executor queue.
If no, do nothing — the slave will be checked again when its next requirement arrives.
The dependency check query:
```sql
SELECT slave_addr FROM slave_req
WHERE req_addr = %s 
AND slave_addr NOT IN (
SELECT slave_addr FROM slave_req
WHERE req_addr NOT IN (SELECT addr FROM results)
);
```
## On startup
The executor queue is in-memory only and does not persist across restarts. On startup the scheduler performs a full scan of all slaves in the DB and pushes any whose requirements are already fully satisfied into the executor queue. This is a one-time cost on startup and avoids the complexity and failure modes of a persisted queue.

## Error handling
Unrecoverable slave goal errors are handled by the interrupt manager, not the scheduler. The scheduler only concerns itself with readiness — it assumes the executor always writes something to results even on failure, ensuring no slave goal is permanently blocked.

## Master goal completion
A master goal is complete when all of its slave goals have results. This is checked after each slave result is registered — if all slaves belonging to a master have entries in results, the master is marked complete.

