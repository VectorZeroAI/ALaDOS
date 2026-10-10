from ALaDOS.lib.Result import add_master_result
import json
import sys
import asyncio

args = json.load(sys.stdin)
text = args.get("text")
if text is None:
    raise ValueError("text not given.")
slave_id = args["slave_id"]
asyncio.run(add_master_result(slave_id, text))
print("")
