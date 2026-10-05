from ALaDOS.lib.Result import write
import json
import sys
import asyncio

args = json.load(sys.stdin)
text = args.get("text")
if text is None:
    raise ValueError("text not given.")
slave_id = args["slave_id"]
result = asyncio.run(write(slave_id, text))
print("")
