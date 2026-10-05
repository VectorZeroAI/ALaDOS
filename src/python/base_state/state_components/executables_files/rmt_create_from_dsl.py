from ALaDOS.lib.Rmt import create_from_dsl
import json
import sys
import asyncio

args = json.load(sys.stdin)
dsl = args.get("dsl")
if dsl is None:
    raise ValueError("dsl not given.")
description = args.get("description")
if description is None:
    raise ValueError("description not given.")
slave_id = args["slave_id"]
name = args.get("name")
addr = asyncio.run(create_from_dsl(slave_id, dsl, description, name))
print(json.dumps({
    "addr": addr
}))
