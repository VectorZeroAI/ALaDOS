from ALaDOS.lib.Executables import edit
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("id not given.")
slave_id = args["slave_id"]
header_change = args.get("header_change")
body_change = args.get("body_change")
new_description = args.get("new_description")
if header_change is None and body_change is None and new_description is None:
    raise ValueError("At least one change must be provided.")
asyncio.run(edit(slave_id, id, header_change, body_change, new_description))
print("")
