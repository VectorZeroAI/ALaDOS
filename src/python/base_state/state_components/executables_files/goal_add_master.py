from ALaDOS.lib.Goal import add_master
import json
import sys
import asyncio

args = json.load(sys.stdin)
instruction = args.get("instruction")
if instruction is None:
    raise ValueError("instruction not given.")
slave_id = args["slave_id"]
required_ids = args.get("required_ids", [])
result_name = args.get("result_name")
addr = asyncio.run(add_master(slave_id, instruction, required_ids, result_name))
print(json.dumps({
    "addr": addr
}))
