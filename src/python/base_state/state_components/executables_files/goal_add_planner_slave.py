from ALaDOS.lib.Goal import add_planner_slave
import json
import sys
import asyncio

args = json.load(sys.stdin)
slave_id = args["slave_id"]
asyncio.run(add_planner_slave(slave_id))
print("")
