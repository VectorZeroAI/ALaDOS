from ALaDOS.lib.Web import post
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
payload = args.get("payload", "")
content = asyncio.run(post(slave_id, url, timeout, return_type, headers, payload))
print(json.dumps({
    "content": content
}))
