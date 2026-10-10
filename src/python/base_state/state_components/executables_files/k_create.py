from ALaDOS.lib.Knowledge import create
import json
import sys
import asyncio

args = json.load(sys.stdin)
content = args.get("content")
if content is None:
    raise ValueError("content not given.")
description = args.get("description")
if description is None:
    raise ValueError("description not given.")
slave_id = args["slave_id"]
name = args.get("name")
addr = asyncio.run(create(slave_id, content, description, name))
print(json.dumps({
    "addr": addr
}))
