from ALaDOS.lib.Context import window_land_by_addr
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("id not given.")
slave_id = args["slave_id"]
asyncio.run(window_land_by_addr(slave_id, id))
print("")
