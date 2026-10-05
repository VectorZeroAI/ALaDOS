from ALaDOS.lib.Rmt import insert_node
import json
import sys
import asyncio

args = json.load(sys.stdin)
rmt_id = args.get("rmt_id")
if not rmt_id:
    raise ValueError("rmt_id not given.")
instruction = args.get("instruction")
if instruction is None:
    raise ValueError("instruction not given.")
slave_id = args["slave_id"]
name = args.get("name")
scope = args.get("scope", "general")
depends_on = args.get("depends_on", [])
required_by = args.get("required_by", [])
result = asyncio.run(insert_node(slave_id, rmt_id, instruction, name, scope, depends_on, required_by))
print(json.dumps({
    "node_addr": result["node_addr"],
    "rmt_addr": result["rmt_addr"]
}))
