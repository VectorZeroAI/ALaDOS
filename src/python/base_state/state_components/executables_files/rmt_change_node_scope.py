from ALaDOS.lib.Rmt import change_node_scope
import json
import sys
import asyncio

args = json.load(sys.stdin)
node_id = args.get("node_id")
if not node_id:
    raise ValueError("node_id not given.")
new_scope = args.get("new_scope")
if new_scope is None:
    raise ValueError("new_scope not given.")
slave_id = args["slave_id"]
asyncio.run(change_node_scope(slave_id, node_id, new_scope))
print("")
