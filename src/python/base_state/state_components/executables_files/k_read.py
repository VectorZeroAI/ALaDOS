from ALaDOS.lib.Knowledge import read
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("Id not given.")
slave_id = args["slave_id"]  # automatically injected
content = asyncio.run(read(id, slave_id))
print(json.dumps({"id": id, "content": content}))
