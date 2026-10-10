from ALaDOS.lib.Context import window_change_size
import json
import sys
import asyncio

args = json.load(sys.stdin)
slave_id = args["slave_id"]
left = args.get("left", 0)
right = args.get("right", 0)
if left == 0 and right == 0:
    raise ValueError("Both left and right are 0, which means this is a no op and is assumed as an upstream error. Check intention.")
result = asyncio.run(window_change_size(slave_id, left, right))
print(json.dumps({
    "size_left": result["left"],
    "size_right": result["right"]
}))
