from ALaDOS.lib.Web import search
import json
import sys
import asyncio

args = json.load(sys.stdin)
query = args.get("query")
if query is None:
    raise ValueError("query not given.")
amount_results = args.get("amount_results")
if amount_results is None:
    raise ValueError("amount_results not given.")
slave_id = args["slave_id"]
results = asyncio.run(search(slave_id, query, amount_results))
print(json.dumps(results))
