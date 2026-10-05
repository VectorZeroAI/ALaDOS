from ALaDOS.lib.Goal import add_slave
import json
import sys
import asyncio

args = json.load(sys.stdin)
instruction = args.get("instruction")
if instruction is None:
    raise ValueError("instruction not given.")
slave_id = args["slave_id"]
slave_type = args.get("slave_type", "general")
required_results_ids = args.get("required_results_ids", [])
slave_name = args.get("slave_name")
result_name = args.get("result_name")
addr = asyncio.run(add_slave(slave_id, instruction, slave_type, required_results_ids, slave_name, result_name))
print(json.dumps({
    "addr": addr
}))
