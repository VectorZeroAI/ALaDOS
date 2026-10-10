from ALaDOS.lib.Executables import create
import json
import sys
import asyncio

args = json.load(sys.stdin)
description = args.get("description")
if description is None:
    raise ValueError("description not given.")
header = args.get("header")
if header is None:
    raise ValueError("header not given.")
body = args.get("body")
if body is None:
    raise ValueError("body not given.")
slave_id = args["slave_id"]
name = args.get("name")
addr = asyncio.run(create(slave_id, description, header, body, name))
print(json.dumps({
    "addr": addr,
    "name": name
}))
