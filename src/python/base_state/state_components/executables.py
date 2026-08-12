#!/usr/bin/env python3
"""
Executables base state file, here all the executables in the base state belong.

They basically wrap all the syscalls and maybe give a bit nicer output string,
basically including what the hell the output is of,
so not just the content for example but also where from.

Addresses should be negative, because they are system internal tools,
and thus system internal addresses are used, and they are always negative integers.
"""

from ..types import Executable
from ..registry import register

register(
    Executable(
        description="Read Knowledge Item.",
        body="""
        from ALaDOS.lib.Knowledge import read
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("Id not given.")
        slave_id = args["slave_id"]  # automatically injected
        content = asyncio.run(read(id, slave_id))
        return f"Knowledge Entry at id {id}, content: {content}."
        """,
        header="""
        args = {
            "id": "knowledge entry id (int or str)."
        }
        """,
        name="K.read"
    )
)

# K.edit
register(
    Executable(
        description="Edit Knowledge Item.",
        body="""
        from ALaDOS.lib.Knowledge import edit
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("Id not given.")
        slave_id = args["slave_id"]
        content_change = args.get("content_change")
        description_change = args.get("description_change")
        if content_change is None and description_change is None:
            raise ValueError("At least one change must be provided.")
        asyncio.run(edit(id, slave_id, content_change, description_change))
        return f"Edited knowledge item {id}."
        """,
        header="""
        args = {
            "id": "knowledge entry id (int or str).",
            "content_change": "SearchAndReplaceBlock (optional).",
            "description_change": "SearchAndReplaceBlock (optional)."
        }
        """,
        name="K.edit"
    )
)

# K.create
register(
    Executable(
        description="Create Knowledge Item.",
        body="""
        from ALaDOS.lib.Knowledge import create
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        content = args.get("content")
        if content is None:
            raise ValueError("content not given.")
        description = args.get("description")
        if description is None:
            raise ValueError("description not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        addr = asyncio.run(create(slave_id, content, description, name))
        return f"Created knowledge entry with address {addr}."
        """,
        header="""
        args = {
            "content": "str, the knowledge content.",
            "description": "str, short description for semantic search.",
            "name": "str (optional)."
        }
        Additional Notes:
            !Name of knowledge item can not be used in required_slave_id of goal.add_slave!
        """,
        name="K.create",
        
    )
)

# Tool.execute
register(
    Executable(
        description="Execute a tool (executable) by ID.",
        body="""
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
        return f"Tool {id} executed successfully. Output:\\n{output}"
        """,
        header="""
        args = {
            "id": "tool id (int or str).",
            "timeout": "int (optional, default 10).",
            "kwargs": "dict (optional)."
        }
        """,
        name="Tool.execute",
        
    )
)

# Tool.create
register(
    Executable(
        description="Create a new Python tool.",
        body="""
        from ALaDOS.lib.Executables import create
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        description = args.get("description")
        if description is None:
            raise ValueError("description not given.")
        header = args.get("header")
        if header is None:
            raise ValueError("header not given.")
        body = args.get("body")
        if body is None:
            raise ValueError("body not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        addr = asyncio.run(create(slave_id, description, header, body, name))
        return f"Created tool with address {addr}."
        """,
        header="""
        args = {
            "description": "str, tool description.",
            "header": "str, header documentation.",
            "body": "str, Python code.",
            "name": "str (optional)."
        }
        """,
        name="Tool.create",
        
    )
)

# Tool.edit
register(
    Executable(
        description="Edit an existing tool.",
        body="""
        from ALaDOS.lib.Executables import edit
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("id not given.")
        slave_id = args["slave_id"]
        header_change = args.get("header_change")
        body_change = args.get("body_change")
        new_description = args.get("new_description")
        if header_change is None and body_change is None and new_description is None:
            raise ValueError("At least one change must be provided.")
        asyncio.run(edit(slave_id, id, header_change, body_change, new_description))
        return f"Edited tool {id}."
        """,
        header="""
        args = {
            "id": "tool id (int or str).",
            "header_change": "SearchAndReplaceBlock (optional).",
            "body_change": "SearchAndReplaceBlock (optional).",
            "new_description": "str (optional)."
        }
        """,
        name="Tool.edit",
        
    )
)

# Rmt.create_from_range
register(
    Executable(
        description="Create RMT from a range of slaves.",
        body="""
        from ALaDOS.lib.Rmt import create_from_range
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        start_id = args.get("start_id")
        if not start_id:
            raise ValueError("start_id not given.")
        end_id = args.get("end_id")
        if not end_id:
            raise ValueError("end_id not given.")
        description = args.get("description")
        if description is None:
            raise ValueError("description not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        addr = asyncio.run(create_from_range(slave_id, start_id, end_id, description, name))
        return f"Created RMT from range with address {addr}."
        """,
        header="""
        args = {
            "start_id": "int or str, start slave address.",
            "end_id": "int or str, end slave address.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        }
        """,
        name="Rmt.create_from_range",
        
    )
)

# Rmt.serialize
register(
    Executable(
        description="Serialize an RMT into DSL and description.",
        body="""
        from ALaDOS.lib.Rmt import serialize
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("id not given.")
        slave_id = args["slave_id"]
        result = asyncio.run(serialize(slave_id, id))
        return f"RMT {id} serialized:\\nDSL: {result['dsl']}\\nDescription: {result['description']}"
        """,
        header="""
        args = {
            "id": "RMT id (int or str)."
        }
        """,
        name="Rmt.serialize",
        
    )
)

# Rmt.create_from_dsl
register(
    Executable(
        description="Create RMT from DSL string.",
        body="""
        from ALaDOS.lib.Rmt import create_from_dsl
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        dsl = args.get("dsl")
        if dsl is None:
            raise ValueError("dsl not given.")
        description = args.get("description")
        if description is None:
            raise ValueError("description not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        addr = asyncio.run(create_from_dsl(slave_id, dsl, description, name))
        return f"Created RMT from DSL with address {addr}."
        """,
        header="""
        args = {
            "dsl": "str, DSL representation.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        }
        """,
        name="Rmt.create_from_dsl",
        
    )
)

# Rmt.create_from_master
register(
    Executable(
        description="Create RMT from an existing master.",
        body="""
        from ALaDOS.lib.Rmt import create_from_master
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        master_id = args.get("master_id")
        if not master_id:
            raise ValueError("master_id not given.")
        description = args.get("description")
        if description is None:
            raise ValueError("description not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        addr = asyncio.run(create_from_master(slave_id, master_id, description, name))
        return f"Created RMT from master with address {addr}."
        """,
        header="""
        args = {
            "master_id": "int or str, master address.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        }
        """,
        name="Rmt.create_from_master",
        
    )
)

# Rmt.edit_description
register(
    Executable(
        description="Edit RMT description.",
        body="""
        from ALaDOS.lib.Rmt import edit_description
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        rmt_id = args.get("rmt_id")
        if not rmt_id:
            raise ValueError("rmt_id not given.")
        new_description = args.get("new_description")
        if new_description is None:
            raise ValueError("new_description not given.")
        slave_id = args["slave_id"]
        asyncio.run(edit_description(slave_id, rmt_id, new_description))
        return f"Updated description of RMT {rmt_id}."
        """,
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "new_description": "str."
        }
        """,
        name="Rmt.edit_description",
        
    )
)

# Rmt.delete_node
register(
    Executable(
        description="Delete a node from an RMT.",
        body="""
        from ALaDOS.lib.Rmt import delete_node
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        rmt_slave_id = args.get("rmt_slave_id")
        if not rmt_slave_id:
            raise ValueError("rmt_slave_id not given.")
        template_id = args.get("template_id")
        if not template_id:
            raise ValueError("template_id not given.")
        slave_id = args["slave_id"]
        concatenate = args.get("concatenate", True)
        asyncio.run(delete_node(slave_id, rmt_slave_id, template_id, concatenate))
        return f"Deleted node {rmt_slave_id} from RMT {template_id}."
        """,
        header="""
        args = {
            "rmt_slave_id": "int or str, node to delete.",
            "template_id": "int or str, RMT template address.",
            "concatenate": "bool (optional, default True)."
        }
        """,
        name="Rmt.delete_node",
        
    )
)

# Rmt.insert_node
register(
    Executable(
        description="Insert a node into an RMT.",
        body="""
        from ALaDOS.lib.Rmt import insert_node
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        rmt_id = args.get("rmt_id")
        if not rmt_id:
            raise ValueError("rmt_id not given.")
        instruction = args.get("instruction")
        if instruction is None:
            raise ValueError("instruction not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        scope = args.get("scope", "general")
        depends_on = args.get("depends_on", [])
        required_by = args.get("required_by", [])
        result = asyncio.run(insert_node(slave_id, rmt_id, instruction, name, scope, depends_on, required_by))
        return f"Inserted node {result['node_addr']} into RMT {result['rmt_addr']}."
        """,
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "instruction": "str, node instruction.",
            "name": "str (optional).",
            "scope": "str (optional, default 'general').",
            "depends_on": "list of int/str (optional).",
            "required_by": "list of int/str (optional)."
        }
        """,
        name="Rmt.insert_node",
        
    )
)

# Rmt.activate_as_master
register(
    Executable(
        description="Activate an RMT as a master.",
        body="""
        from ALaDOS.lib.Rmt import activate_as_master
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        rmt_id = args.get("rmt_id")
        if not rmt_id:
            raise ValueError("rmt_id not given.")
        inputs = args.get("inputs")
        if inputs is None:
            raise ValueError("inputs not given.")
        slave_id = args["slave_id"]
        depends_on = args.get("depends_on", [])
        required_by = args.get("required_by", [])
        addr = asyncio.run(activate_as_master(slave_id, rmt_id, inputs, depends_on, required_by))
        return f"Activated RMT {rmt_id} as master with address {addr}."
        """,
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "inputs": "dict, variable substitutions.",
            "depends_on": "list of int/str (optional).",
            "required_by": "list of int/str (optional)."
        }
        """,
        name="Rmt.activate_as_master",
        
    )
)

# Rmt.edit_node_instruction
register(
    Executable(
        description="Edit instruction of an RMT node.",
        body="""
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
        return f"Edited instruction of RMT node {node_id}."
        """,
        header="""
        args = {
            "node_id": "int or str, RMT slave node address.",
            "sr_block": "SearchAndReplaceBlock."
        }
        """,
        name="Rmt.edit_node_instruction",
        
    )
)

# Rmt.change_node_scope
register(
    Executable(
        description="Change scope of an RMT node.",
        body="""
        from ALaDOS.lib.Rmt import change_node_scope
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        node_id = args.get("node_id")
        if not node_id:
            raise ValueError("node_id not given.")
        new_scope = args.get("new_scope")
        if new_scope is None:
            raise ValueError("new_scope not given.")
        slave_id = args["slave_id"]
        asyncio.run(change_node_scope(slave_id, node_id, new_scope))
        return f"Changed scope of RMT node {node_id} to {new_scope}."
        """,
        header="""
        args = {
            "node_id": "int or str, RMT slave node address.",
            "new_scope": "str, e.g. 'general', 'task', etc."
        }
        """,
        name="Rmt.change_node_scope",
        
    )
)

# Rmt.register_reaction_rmt
register(
    Executable(
        description="Register an RMT as reaction to an event.",
        body="""
        from ALaDOS.lib.Rmt import register_reaction_rmt
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        rmt_id = args.get("rmt_id")
        if not rmt_id:
            raise ValueError("rmt_id not given.")
        args_dict = args.get("args", {})
        slave_id = args["slave_id"]
        consumer_addr = asyncio.run(register_reaction_rmt(slave_id, event_path, rmt_id, args_dict))
        return f"Registered RMT {rmt_id} for event {event_path}, consumer address {consumer_addr}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "rmt_id": "int or str, RMT address.",
            "args": "dict (optional, arguments for RMT activation)."
        }
        """,
        name="Rmt.register_reaction_rmt",
        
    )
)

# Rmt.register_reaction_slave
register(
    Executable(
        description="Register a single slave as reaction to an event.",
        body="""
        from ALaDOS.lib.Rmt import register_reaction_slave
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        instruction = args.get("instruction")
        if instruction is None:
            raise ValueError("instruction not given.")
        slave_id = args["slave_id"]
        scope = args.get("scope", "general")
        consumer_addr = asyncio.run(register_reaction_slave(slave_id, event_path, instruction, scope))
        return f"Registered slave reaction for event {event_path}, consumer address {consumer_addr}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "instruction": "str, instruction for the slave.",
            "scope": "str (optional, default 'general')."
        }
        """,
        name="Rmt.register_reaction_slave",
        
    )
)

# Rmt.create_result_via_event
register(
    Executable(
        description="Create a result that will be filled by an event.",
        body="""
        from ALaDOS.lib.Rmt import create_result_via_event
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        result_str = args.get("result_str")
        if result_str is None:
            raise ValueError("result_str not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        result = asyncio.run(create_result_via_event(slave_id, event_path, result_str, name))
        return f"Created event-driven result {result['result_addr']} with consumer {result['consumer_addr']}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "result_str": "str, template with ${{data}} and ${{event}}.",
            "name": "str (optional)."
        }
        """,
        name="Rmt.create_result_via_event",
        
    )
)

# Context.add
register(
    Executable(
        description="Add an item to the current context.",
        body="""
        from ALaDOS.lib.Context import add
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("id not given.")
        slave_id = args["slave_id"]
        asyncio.run(add(slave_id, id))
        return f"Added item {id} to context."
        """,
        header="""
        args = {
            "id": "int or str, address or name of item."
        }
        """,
        name="Context.add",
        
    )
)

# Context.window_semantic_land
register(
    Executable(
        description="Land context window on semantically similar item.",
        body="""
        from ALaDOS.lib.Context import window_semantic_land
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        query = args.get("query")
        if query is None:
            raise ValueError("query not given.")
        slave_id = args["slave_id"]
        anchor = asyncio.run(window_semantic_land(slave_id, query))
        return f"Semantic land on query '{query}' set anchor to {anchor}."
        """,
        header="""
        args = {
            "query": "str, search query."
        }
        """,
        name="Context.window_semantic_land",
        
    )
)

# Context.window_land_by_addr
register(
    Executable(
        description="Land context window directly on an item by address.",
        body="""
        from ALaDOS.lib.Context import window_land_by_addr
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("id not given.")
        slave_id = args["slave_id"]
        asyncio.run(window_land_by_addr(slave_id, id))
        return f"Landed context window on item {id}."
        """,
        header="""
        args = {
            "id": "int or str, address or name."
        }
        """,
        name="Context.window_land_by_addr",
        
    )
)

# Context.window_change_size
register(
    Executable(
        description="Change the size of the context window.",
        body="""
        from ALaDOS.lib.Context import window_change_size
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        slave_id = args["slave_id"]
        left = args.get("left", 0)
        right = args.get("right", 0)
        result = asyncio.run(window_change_size(slave_id, left, right))
        return f"Changed window size: left={result['left']}, right={result['right']}."
        """,
        header="""
        args = {
            "left": "int (optional, default 0).",
            "right": "int (optional, default 0)."
        }
        """,
        name="Context.window_change_size",
        
    )
)

# Context.window_move_anchor
register(
    Executable(
        description="Move the context window anchor.",
        body="""
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
        return f"Moved anchor by {amount}, new anchor: {new_anchor}."
        """,
        header="""
        args = {
            "amount": "int, positive moves right, negative left."
        }
        """,
        name="Context.window_move_anchor",
        
    )
)

# Context.unload_item
register(
    Executable(
        description="Unload an item from the context.",
        body="""
        from ALaDOS.lib.Context import unload_item
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        id = args.get("id")
        if not id:
            raise ValueError("id not given.")
        slave_id = args["slave_id"]
        asyncio.run(unload_item(slave_id, id))
        return f"Unloaded item {id} from context."
        """,
        header="""
        args = {
            "id": "int or str, address or name."
        }
        """,
        name="Context.unload_item",
        
    )
)

# Goal.add_slave
register(
    Executable(
        description="Add a slave step to the current master.",
        body="""
        from ALaDOS.lib.Goal import add_slave
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        instruction = args.get("instruction")
        if instruction is None:
            raise ValueError("instruction not given.")
        slave_id = args["slave_id"]
        slave_type = args.get("slave_type", "general")
        required_results_ids = args.get("required_results_ids", [])
        slave_name = args.get("slave_name")
        result_name = args.get("result_name")
        addr = asyncio.run(add_slave(slave_id, instruction, slave_type, required_results_ids, slave_name, result_name))
        return f"Added slave step with address {addr}."
        """,
        header="""
        args = {
            "instruction": "str, the slave instruction.",
            "slave_type": "str (optional, default 'general').",
            "required_results_ids": "list of int/str (optional).",
            "slave_name": "str (optional).",
            "result_name": "str (optional)."
        }
        Usage Notes:
            required_results_ids may include 'self', which would refer to the current slave.
        """,
        name="Goal.add_slave",
        
    )
)

# Goal.add_planner_slave
register(
    Executable(
        description="Add a planner slave to incrementally plan the master.",
        body="""
        from ALaDOS.lib.Goal import add_planner_slave
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        slave_id = args["slave_id"]
        asyncio.run(add_planner_slave(slave_id))
        return "Added planner slave."
        """,
        header="""
        args = {}
        """,
        name="Goal.add_planner_slave",
        
    )
)

# Goal.add_master
register(
    Executable(
        description="Create a new master goal.",
        body="""
        from ALaDOS.lib.Goal import add_master
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        instruction = args.get("instruction")
        if instruction is None:
            raise ValueError("instruction not given.")
        slave_id = args["slave_id"]
        required_ids = args.get("required_ids", [])
        result_name = args.get("result_name")
        addr = asyncio.run(add_master(slave_id, instruction, required_ids, result_name))
        return f"Created master goal with address {addr}."
        """,
        header="""
        args = {
            "instruction": "str, master instruction.",
            "required_ids": "list of int/str (optional).",
            "result_name": "str (optional)."
        }
        """,
        name="Goal.add_master",
        
    )
)

# Goal.add_cron_job
register(
    Executable(
        description="Add a cron job (once or loop).",
        body="""
        from ALaDOS.lib.Goal import add_cron_job
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        cronjob_type = args.get("cronjob_type")
        if cronjob_type not in ("once", "loop"):
            raise ValueError("cronjob_type must be 'once' or 'loop'.")
        action = args.get("action")
        if action is None:
            raise ValueError("action not given.")
        time_between_runs = args.get("time_between_runs")
        if time_between_runs is None:
            raise ValueError("time_between_runs not given.")
        params = args.get("params", {})
        slave_id = args["slave_id"]
        addr = asyncio.run(add_cron_job(slave_id, cronjob_type, action, time_between_runs, params))
        return f"Added cron job with address {addr}."
        """,
        header="""
        args = {
            "cronjob_type": "'once' or 'loop'.",
            "action": "str, e.g. 'do_this_later'.",
            "time_between_runs": "int, seconds.",
            "params": "dict (optional)."
        }
        """,
        name="Goal.add_cron_job",
        
    )
)

# Result.add_master_result
register(
    Executable(
        description="Append text to the master result.",
        body="""
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
        return f"Appended to master result: {text}"
        """,
        header="""
        args = {
            "text": "str, text to append."
        }
        """,
        name="Result.add_master_result",
        
    )
)

# Result.write
register(
    Executable(
        description="Write the result of the current slave instruction.",
        body="""
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
        return f"Slave result written: {result}"
        """,
        header="""
        args = {
            "text": "str, result text."
        }
        """,
        name="Result.write",
        
    )
)

# Web.search_fulltext
register(
    Executable(
        description="Search web and return full text of top pages.",
        body="""
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
        return f"Full text search results for '{query}':\\n{content}"
        """,
        header="""
        args = {
            "query": "str, search query.",
            "websites_amount": "int (optional, default 3)."
        }
        """,
        name="Web.search_fulltext",
        
    )
)

# Web.search
register(
    Executable(
        description="Search web and return list of URLs with titles and snippets.",
        body="""
        from ALaDOS.lib.Web import search
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        query = args.get("query")
        if query is None:
            raise ValueError("query not given.")
        amount_results = args.get("amount_results")
        if amount_results is None:
            raise ValueError("amount_results not given.")
        slave_id = args["slave_id"]
        results = asyncio.run(search(slave_id, query, amount_results))
        return f"Web search results for '{query}':\\n{json.dumps(results, indent=2)}"
        """,
        header="""
        args = {
            "query": "str, search query.",
            "amount_results": "int, number of results."
        }
        """,
        name="Web.search",
        
    )
)

# Web.get
register(
    Executable(
        description="Perform HTTP GET request.",
        body="""
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
        return f"GET {url} returned:\\n{content}"
        """,
        header="""
        args = {
            "url": "str, URL.",
            "timeout": "int (optional, default 10).",
            "return_type": "'extracted' or 'raw' (optional, default 'extracted').",
            "headers": "dict (optional)."
        }
        """,
        name="Web.get",
        
    )
)

# Web.post
register(
    Executable(
        description="Perform HTTP POST request.",
        body="""
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
        return f"POST {url} returned:\\n{content}"
        """,
        header="""
        args = {
            "url": "str, URL.",
            "timeout": "int (optional, default 10).",
            "return_type": "'extracted', 'raw', or 'status_code' (optional, default 'extracted').",
            "headers": "dict (optional).",
            "payload": "str (optional)."
        }
        """,
        name="Web.post",
        
    )
)

# Event.register_reaction_rmt
register(
    Executable(
        description="Register an RMT as reaction to an event (Event module).",
        body="""
        from ALaDOS.lib.Event import register_reaction_rmt
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        rmt_id = args.get("rmt_id")
        if not rmt_id:
            raise ValueError("rmt_id not given.")
        args_dict = args.get("args", {})
        slave_id = args["slave_id"]
        consumer_addr = asyncio.run(register_reaction_rmt(slave_id, event_path, rmt_id, args_dict))
        return f"Registered RMT {rmt_id} for event {event_path}, consumer address {consumer_addr}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "rmt_id": "int or str, RMT address.",
            "args": "dict (optional)."
        }
        """,
        name="Event.register_reaction_rmt",
        
    )
)

# Event.register_reaction_slave
register(
    Executable(
        description="Register a slave as reaction to an event (Event module).",
        body="""
        from ALaDOS.lib.Event import register_reaction_slave
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        instruction = args.get("instruction")
        if instruction is None:
            raise ValueError("instruction not given.")
        slave_id = args["slave_id"]
        scope = args.get("scope", "general")
        consumer_addr = asyncio.run(register_reaction_slave(slave_id, event_path, instruction, scope))
        return f"Registered slave reaction for event {event_path}, consumer address {consumer_addr}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "instruction": "str, slave instruction.",
            "scope": "str (optional, default 'general')."
        }
        """,
        name="Event.register_reaction_slave",
        
    )
)

# Event.create_result
register(
    Executable(
        description="Create a result filled by an event (Event module).",
        body="""
        from ALaDOS.lib.Event import create_result
        import json
        import sys
        import asyncio
        
        args = json.load(sys.stdin)
        event_path = args.get("event_path")
        if event_path is None:
            raise ValueError("event_path not given.")
        result_str = args.get("result_str")
        if result_str is None:
            raise ValueError("result_str not given.")
        slave_id = args["slave_id"]
        name = args.get("name")
        result = asyncio.run(create_result(slave_id, event_path, result_str, name))
        return f"Created event-driven result {result['result_addr']} with consumer {result['consumer_addr']}."
        """,
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "result_str": "str, template with ${{data}} and ${{event}}.",
            "name": "str (optional)."
        }
        """,
        name="Event.create_result",
        
    )
)

# Report.report_paradoxal_information
register(
    Executable(
        description="Report paradoxical information (aborts execution).",
        body="""
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
        # This call will raise an exception and never return normally
        return "Reported paradox (should not reach here)."
        """,
        header="""
        args = {
            "items": "list of int/str, addresses or names of paradoxical items.",
            "paradox": "str, description of the paradox."
        }
        """,
        name="Report.report_paradoxal_information",
    )
)
