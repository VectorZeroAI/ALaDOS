from ALaDOS.lib.Web import get
import json
import sys
import asyncio

args = json.load(sys.stdin)
url = args.get("url")
if url is None:
    raise ValueError("url not given.")
slave_id = args["slave_id"]
timeout = args.get("timeout", 10)
return_type = args.get("return_type", "extracted")
headers = args.get("headers", {})
content = asyncio.run(get(slave_id, url, timeout, return_type, headers))
print(json.dumps({
    "content": content
}))
