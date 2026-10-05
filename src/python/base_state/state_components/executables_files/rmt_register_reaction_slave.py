from ALaDOS.lib.Rmt import register_reaction_slave
import json
import sys
import asyncio

args = json.load(sys.stdin)
event_path = args.get("event_path")
if event_path is None:
    raise ValueError("event_path not given.")
instruction = args.get("instruction")
if instruction is None:
    raise ValueError("instruction not given.")
slave_id = args["slave_id"]
scope = args.get("scope", "general")
consumer_addr = asyncio.run(register_reaction_slave(slave_id, event_path, instruction, scope))
print(json.dumps({
    "consumer_addr": consumer_addr
}))
