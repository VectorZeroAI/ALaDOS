from ALaDOS.lib.Knowledge import edit
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("Id not given.")
slave_id = args["slave_id"]
content_change = args.get("content_change")
description_change = args.get("description_change")
if content_change is None and description_change is None:
    raise ValueError("At least one change must be provided.")
asyncio.run(edit(id, slave_id, content_change, description_change))
print("")
