from ALaDOS.lib.Context import add
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("id not given.")
slave_id = args["slave_id"]
asyncio.run(add(slave_id, id))
print("")
