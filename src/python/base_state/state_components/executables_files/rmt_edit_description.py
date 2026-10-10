from ALaDOS.lib.Rmt import edit_description
import json
import sys
import asyncio

args = json.load(sys.stdin)
rmt_id = args.get("rmt_id")
if not rmt_id:
    raise ValueError("rmt_id not given.")
new_description = args.get("new_description")
if new_description is None:
    raise ValueError("new_description not given.")
slave_id = args["slave_id"]
asyncio.run(edit_description(slave_id, rmt_id, new_description))
print("")
