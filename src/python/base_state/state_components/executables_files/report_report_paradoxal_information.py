from ALaDOS.lib.Report import report_paradoxal_information
import json
import sys
import asyncio

args = json.load(sys.stdin)
items = args.get("items")
if items is None:
    raise ValueError("items not given.")
paradox = args.get("paradox")
if paradox is None:
    raise ValueError("paradox not given.")
slave_id = args["slave_id"]
asyncio.run(report_paradoxal_information(slave_id, items, paradox))
print("")
