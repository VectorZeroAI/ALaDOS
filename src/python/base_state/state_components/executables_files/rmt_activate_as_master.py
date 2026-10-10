from ALaDOS.lib.Rmt import activate_as_master
import json
import sys
import asyncio

args = json.load(sys.stdin)
rmt_id = args.get("rmt_id")
if not rmt_id:
    raise ValueError("rmt_id not given.")
inputs = args.get("inputs")
if inputs is None:
    raise ValueError("inputs not given.")
slave_id = args["slave_id"]
depends_on = args.get("depends_on", [])
required_by = args.get("required_by", [])
addr = asyncio.run(activate_as_master(slave_id, rmt_id, inputs, depends_on, required_by))
print(json.dumps({
    "rmt_id": rmt_id,
    "master_addr": addr
}))
