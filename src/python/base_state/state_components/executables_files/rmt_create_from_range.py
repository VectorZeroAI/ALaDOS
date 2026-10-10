from ALaDOS.lib.Rmt import create_from_range
import json
import sys
import asyncio

args = json.load(sys.stdin)
start_id = args.get("start_id")
if not start_id:
    raise ValueError("start_id not given.")
end_id = args.get("end_id")
if not end_id:
    raise ValueError("end_id not given.")
description = args.get("description")
if description is None:
    raise ValueError("description not given.")
slave_id = args["slave_id"]
name = args.get("name")
addr = asyncio.run(create_from_range(slave_id, start_id, end_id, description, name))
print(json.dumps({
    "addr": addr,
    "name": name
}))
