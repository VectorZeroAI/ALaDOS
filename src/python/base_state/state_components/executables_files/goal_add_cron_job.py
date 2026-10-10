from ALaDOS.lib.Goal import add_cron_job
import json
import sys
import asyncio

args = json.load(sys.stdin)
cronjob_type = args.get("cronjob_type")
if cronjob_type not in ("once", "loop"):
    raise ValueError("cronjob_type must be 'once' or 'loop'.")
action = args.get("action")
if action is None:
    raise ValueError("action not given.")
time_between_runs = args.get("time_between_runs")
if time_between_runs is None:
    raise ValueError("time_between_runs not given.")
params = args.get("params", {})
slave_id = args["slave_id"]
addr = asyncio.run(add_cron_job(slave_id, cronjob_type, action, time_between_runs, params))
print(json.dumps({
    "addr": addr
}))
