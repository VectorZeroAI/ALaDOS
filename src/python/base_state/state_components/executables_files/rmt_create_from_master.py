from ALaDOS.lib.Rmt import create_from_master
import json
import sys
import asyncio

args = json.load(sys.stdin)
master_id = args.get("master_id")
if not master_id:
    raise ValueError("master_id not given.")
description = args.get("description")
if description is None:
    raise ValueError("description not given.")
slave_id = args["slave_id"]
name = args.get("name")
addr = asyncio.run(create_from_master(slave_id, master_id, description, name))
print(json.dumps({
    "addr": addr
}))
