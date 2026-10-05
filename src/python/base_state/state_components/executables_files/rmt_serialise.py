from ALaDOS.lib.Rmt import serialize
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("id not given.")
slave_id = args["slave_id"]
result = asyncio.run(serialize(slave_id, id))
print(json.dumps({
    "id": id,
    "dsl": result["dsl"],
    "description": result["description"]
}))
