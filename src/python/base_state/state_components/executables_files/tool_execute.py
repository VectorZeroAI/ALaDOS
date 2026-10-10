from ALaDOS.lib.Executables import execute
import json
import sys
import asyncio

args = json.load(sys.stdin)
id = args.get("id")
if not id:
    raise ValueError("id not given.")
slave_id = args["slave_id"]
timeout = args.get("timeout", 10)
kwargs = args.get("kwargs", {})
output = asyncio.run(execute(slave_id, id, timeout, kwargs))

print(json.dumps({
    "id": id,
    "output": output
}))
