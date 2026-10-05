from ALaDOS.lib.Rmt import delete_node
import json
import sys
import asyncio

args = json.load(sys.stdin)
rmt_slave_id = args.get("rmt_slave_id")
if not rmt_slave_id:
    raise ValueError("rmt_slave_id not given.")
template_id = args.get("template_id")
if not template_id:
    raise ValueError("template_id not given.")
slave_id = args["slave_id"]
concatenate = args.get("concatenate", True)
asyncio.run(delete_node(slave_id, rmt_slave_id, template_id, concatenate))
print(json.dumps({
    "slave_id": rmt_slave_id,
    "template_id": template_id
}))
