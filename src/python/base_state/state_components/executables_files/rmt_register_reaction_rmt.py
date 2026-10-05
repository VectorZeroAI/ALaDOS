from ALaDOS.lib.Rmt import register_reaction_rmt
import json
import sys
import asyncio

args = json.load(sys.stdin)
event_path = args.get("event_path")
if event_path is None:
    raise ValueError("event_path not given.")
rmt_id = args.get("rmt_id")
if not rmt_id:
    raise ValueError("rmt_id not given.")
args_dict = args.get("args", {})
slave_id = args["slave_id"]
consumer_addr = asyncio.run(register_reaction_rmt(slave_id, event_path, rmt_id, args_dict))
print(json.dumps({
    "consumer_addr": consumer_addr
}))
