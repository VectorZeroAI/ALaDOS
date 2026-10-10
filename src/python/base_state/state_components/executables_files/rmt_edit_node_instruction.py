from ALaDOS.lib.Rmt import edit_node_instruction
import json
import sys
import asyncio

args = json.load(sys.stdin)
node_id = args.get("node_id")
if not node_id:
    raise ValueError("node_id not given.")
sr_block = args.get("sr_block")
if sr_block is None:
    raise ValueError("sr_block not given.")
slave_id = args["slave_id"]
asyncio.run(edit_node_instruction(slave_id, node_id, sr_block))
print("")
