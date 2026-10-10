from ALaDOS.lib.Context import window_move_anchor
import json
import sys
import asyncio

args = json.load(sys.stdin)
slave_id = args["slave_id"]
amount = args.get("amount")
if amount is None:
    raise ValueError("amount not given.")
new_anchor = asyncio.run(window_move_anchor(slave_id, amount))
print(json.dumps({
    "new_anchor": new_anchor
}))
