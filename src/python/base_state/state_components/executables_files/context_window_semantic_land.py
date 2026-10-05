from ALaDOS.lib.Context import window_semantic_land
import json
import sys
import asyncio

args = json.load(sys.stdin)
query = args.get("query")
if query is None:
    raise ValueError("query not given.")
slave_id = args["slave_id"]
anchor = asyncio.run(window_semantic_land(slave_id, query))
print(json.dumps({
    "anchor_addr": anchor
}))
