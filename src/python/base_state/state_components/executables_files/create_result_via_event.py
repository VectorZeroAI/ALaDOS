from ALaDOS.lib.Rmt import create_result_via_event
import json
import sys
import asyncio

args = json.load(sys.stdin)
event_path = args.get("event_path")
if event_path is None:
    raise ValueError("event_path not given.")
result_str = args.get("result_str")
if result_str is None:
    raise ValueError("result_str not given.")
slave_id = args["slave_id"]
name = args.get("name")
result = asyncio.run(create_result_via_event(slave_id, event_path, result_str, name))
print(json.dumps({
    "result_addr": result["result_addr"],
    "consumer_addr": result["consumer_addr"]
}))
