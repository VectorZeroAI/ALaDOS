from ALaDOS.lib.Web import search_fulltext
import json
import sys
import asyncio

args = json.load(sys.stdin)
query = args.get("query")
if query is None:
    raise ValueError("query not given.")
slave_id = args["slave_id"]
websites_amount = args.get("websites_amount", 3)
content = asyncio.run(search_fulltext(slave_id, query, websites_amount))
print(json.dumps({
    "full_result": content
}))
