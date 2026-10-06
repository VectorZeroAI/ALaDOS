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
from pathlib import Path

register(
    Executable(
        description="Read Knowledge Item.",
        body=Path(__file__) / "executable_files" / "k_read.py.py",
        header="""
        args = {
            "id": "knowledge entry id (int or str)."
        },
        returns = {"id": "id", "content": "content of the knowledge entry."}
        """,
        name="K.read"
    )
)

# k_edit
register(
    Executable(
        description="Edit Knowledge Item.",
        body=Path(__file__) / "executable_files" / "k_edit.py",
        header="""
        args = {
            "id": "knowledge entry id (int or str).",
            "content_change": "SearchAndReplaceBlock (optional).",
            "description_change": "SearchAndReplaceBlock (optional)."
        },
        returns = ""
        """,
        name="K.edit"
    )
)

# k_create
register(
    Executable(
        description="Create Knowledge Item.",
        body=Path(__file__) / "executable_files" / "k_create.py",
        header="""
        args = {
            "content": "str, the knowledge content.",
            "description": "str, short description for semantic search.",
            "name": "str (optional)."
        },
        returns = {"addr": "addr to coerse."}
        Additional Notes:
            !Name of knowledge item can not be used in required_slave_id of goal.add_slave!
        """,
        name="K.create",
        
    )
)

# tool_execute
register(
    Executable(
        description="Execute a tool (executable) by ID.",
        body=Path(__file__) / "executable_files" / "tool_execute.py",
        header="""
        args = {
            "id": "tool id (int or str).",
            "timeout": "int (optional, default 10).",
            "kwargs": "dict (optional)."
        },
        returns = {
            "id": "id",
            "output": "output"
        }
        """,
        name="Tool.execute",
        
    )
)

# tool_create
register(
    Executable(
        description="Create a new Python tool.",
        body=Path(__file__) / "executable_files" / "tool_create.py",
        header="""
        args = {
            "description": "str, tool description.",
            "header": "str, header documentation.",
            "body": "str, Python code.",
            "name": "str (optional)."
        },
        returns = {
            "addr": "addr to coerse",
            "name": "name str"
        }
        """,
        name="Tool.create",
        
    )
)

# tool_edit
register(
    Executable(
        description="Edit an existing tool.",
        body=Path(__file__) / "executable_files" / "tool_edit.py",
        header="""
        args = {
            "id": "tool id (int or str).",
            "header_change": "SearchAndReplaceBlock (optional).",
            "body_change": "SearchAndReplaceBlock (optional).",
            "new_description": "str (optional)."
        },
        returns = ""
        NOTES: 
            !One of the changes must be present!
        """,
        name="Tool.edit",
        
    )
)

# rmt_create_from_range
register(
    Executable(
        description="Create RMT from a range of slaves.",
        body=Path(__file__) / "executable_files" / "rmt_create_from_range.py",
        header="""
        args = {
            "start_id": "int or str, start slave address.",
            "end_id": "int or str, end slave address.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        },
        returns = {
            "addr": "addr to coerse",
            "name": "name str"
        }
        """,
        name="Rmt.create_from_range",
        
    )
)

# rmt_serialize
register(
    Executable(
        description="Serialize an RMT into DSL and description.",
        body=Path(__file__) / "executable_files" / "rmt_serialise.py",
        header="""
        args = {
            "id": "RMT id (int or str)."
        },
        returns = {
            "id": "id",
            "dsl": "dsl string",
            "description": "description string"
        }
        """,
        name="Rmt.serialize",
        
    )
)

# rmt_create_from_dsl
register(
    Executable(
        description="Create RMT from DSL string.",
        body=Path(__file__) / "executable_files" / "rmt_create_from_dsl.py",
        header="""
        args = {
            "dsl": "str, DSL representation.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        },
        returns = {
            "addr": "addr to coerse"
        }
        """,
        name="Rmt.create_from_dsl",
        
    )
)

# rmt_create_from_master
register(
    Executable(
        description="Create RMT from an existing master.",
        body=Path(__file__) / "executable_files" / "rmt_create_from_master.py",
        header="""
        args = {
            "master_id": "int or str, master address.",
            "description": "str, RMT description.",
            "name": "str (optional)."
        }, 
        returns = {
            "addr": "addr to coerse"
        }
        """,
        name="Rmt.create_from_master",
        
    )
)

# rmt_edit_description
register(
    Executable(
        description="Edit RMT description.",
        body=Path(__file__) / "executable_files" / "rmt_edit_description.py",
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "new_description": "str."
        }, 
        returns = ""
        """,
        name="Rmt.edit_description",
        
    )
)

# rmt_delete_node
register(
    Executable(
        description="Delete a node from an RMT.",
        body=Path(__file__) / "executable_files" / "rmt_delete_node.py",
        header="""
        args = {
            "rmt_slave_id": "int or str, node to delete.",
            "template_id": "int or str, RMT template address.",
            "concatenate": "bool (optional, default True)."
        },
        returns = {
            "slave_id": "slave id",
            "template_id": "template id"
        }
        """,
        name="Rmt.delete_node",
        
    )
)

# rmt_insert_node
register(
    Executable(
        description="Insert a node into an RMT.",
        body=Path(__file__) / "executable_files" / "rmt_insert_node.py",
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "instruction": "str, node instruction.",
            "name": "str (optional).",
            "scope": "str (optional, default 'general').",
            "depends_on": "list of int/str (optional).",
            "required_by": "list of int/str (optional)."
        }, 
        returns = {
            "node_addr": "addr to coerse",
            "rmt_addr": "addr to coerse"
        }
        """,
        name="Rmt.insert_node",
        
    )
)

# rmt_activate_as_master
register(
    Executable(
        description="Activate an RMT as a master.",
        body=Path(__file__) / "executable_files" / "rmt_activate_as_master.py",
        header="""
        args = {
            "rmt_id": "int or str, RMT address.",
            "inputs": "dict, variable substitutions.",
            "depends_on": "list of int/str (optional).",
            "required_by": "list of int/str (optional)."
        }, 
        returns = {
            "rmt_id": "rmt id",
            "master_addr": "master_addr to coerse"
        }
        """,
        name="Rmt.activate_as_master",
        
    )
)

# rmt_edit_node_instruction
register(
    Executable(
        description="Edit instruction of an RMT node.",
        body=Path(__file__) / "executable_files" / "rmt_edit_node_instruction.py",
        header="""
        args = {
            "node_id": "int or str, RMT slave node address.",
            "sr_block": "SearchAndReplaceBlock."
        },
        returns = ""
        """,
        name="Rmt.edit_node_instruction",
        
    )
)

# rmt_change_node_scope
register(
    Executable(
        description="Change scope of an RMT node.",
        body=Path(__file__) / "executable_files" / "rmt_change_node_scope.py",
        header="""
        args = {
            "node_id": "int or str, RMT slave node address.",
            "new_scope": "str, e.g. 'general', 'task', etc."
        },
        returns = ""
        """,
        name="Rmt.change_node_scope",
        
    )
)

# rmt_register_reaction_rmt
register(
    Executable(
        description="Register an RMT as reaction to an event.",
        body=Path(__file__) / "executable_files" / "rmt_register_reaction_rmt.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "rmt_id": "int or str, RMT address.",
            "args": "dict (optional, arguments for RMT activation)."
        },
        returns = {
            "consumer_addr": "consumer_addr to coerse"
        }
        """,
        name="Rmt.register_reaction_rmt",
        
    )
)

# rmt_register_reaction_slave
register(
    Executable(
        description="Register a single slave as reaction to an event.",
        body=Path(__file__) / "executable_files" / "rmt_register_reaction_slave.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "instruction": "str, instruction for the slave.",
            "scope": "str (optional, default 'general')."
        },
        returns = {
            "consumer_addr": "consumer_addr to coerse"
        }
        """,
        name="Rmt.register_reaction_slave",
        
    )
)

# rmt_create_result_via_event
register(
    Executable(
        description="Create a result that will be filled by an event.",
        body=Path(__file__) / "executable_files" / "rmt_create_result_via_event.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "result_str": "str, template with ${{data}} and ${{event}}.",
            "name": "str (optional)."
        }, 
        returns = {
            "result_addr": "result addr to coerse",
            "consumer_addr": "consumer addr to coerse"
        }
        """,
        name="Rmt.create_result_via_event",
        
    )
)

# context_add
register(
    Executable(
        description="Add an item to the current context.",
        body=Path(__file__) / "executable_files" / "context_add.py",
        header="""
        args = {
            "id": "int or str, address or name of item."
        },
        returns = ""
        """,
        name="Context.add",
        
    )
)

# context_window_semantic_land
register(
    Executable(
        description="Land context window on semantically similar item.",
        body=Path(__file__) / "executable_files" / "context_window_semantic_land.py",
        header="""
        args = {
            "query": "str, search query."
        },
        returns = {
            "anchor_addr": "addr to coerse"
        }
        """,
        name="Context.window_semantic_land",
        
    )
)

# context_window_land_by_addr
register(
    Executable(
        description="Land context window directly on an item by address.",
        body=Path(__file__) / "executable_files" / "context_window_land_by_addr.py",
        header="""
        args = {
            "id": "int or str, address or name."
        },
        returns = ""
        """,
        name="Context.window_land_by_addr",
        
    )
)

# context_window_change_size
register(
    Executable(
        description="Change the size of the context window.",
        body=Path(__file__) / "executable_files" / "context_window_change_size.py",
        header="""
        args = {
            "left": "int (optional, default 0).",
            "right": "int (optional, default 0)."
        },
        returns = {
            "size_left": "new size to the left to coerse to int",
            "size_right": "new size to the right to coerse to int"
        }
        """,
        name="Context.window_change_size",
        
    )
)

# context_window_move_anchor
register(
    Executable(
        description="Move the context window anchor.",
        body=Path(__file__) / "executable_files" / "context_window_move_anchor.py",
        header="""
        args = {
            "amount": "int, positive moves right, negative left."
        },
        returns = {
            "new_anchor": "new anchor address to coerse"
        }
        """,
        name="Context.window_move_anchor",
        
    )
)

# context_unload_item
register(
    Executable(
        description="Unload an item from the context.",
        body=Path(__file__) / "executable_files" / "context_unload_item.py",
        header="""
        args = {
            "id": "int or str, address or name."
        },
        returns = ""
        """,
        name="Context.unload_item",
        
    )
)

# goal_add_slave
register(
    Executable(
        description="Add a slave step to the current master.",
        body=Path(__file__) / "executable_files" / "goal_add_slave.py",
        header="""
        args = {
            "instruction": "str, the slave instruction.",
            "slave_type": "str (optional, default 'general').",
            "required_results_ids": "list of int/str (optional).",
            "slave_name": "str (optional).",
            "result_name": "str (optional)."
        },
        returns = {
            "addr": "slave addr to coerse"
        }
        Usage Notes:
            required_results_ids may include 'self', which would refer to the current slave.
        """,
        name="Goal.add_slave",
        
    )
)

# goal_add_planner_slave
register(
    Executable(
        description="Add a planner slave to incrementally plan the master.",
        body=Path(__file__) / "executable_files" / "goal_add_planner_slave",
        header="""
        args = {},
        returns = ""
        """,
        name="Goal.add_planner_slave",
        
    )
)

# goal_add_master
register(
    Executable(
        description="Create a new master goal.",
        body=Path(__file__) / "executable_files" / "goal_add_master.py",
        header="""
        args = {
            "instruction": "str, master instruction.",
            "required_ids": "list of int/str (optional).",
            "result_name": "str (optional)."
        },
        returns = {
            "addr": "addr to coerse"
        }
        """,
        name="Goal.add_master",
        
    )
)

# goal_add_cron_job
register(
    Executable(
        description="Add a cron job (once or loop).",
        body=Path(__file__) / "executable_files" / "goal_add_cron_job.py",
        header="""
        args = {
            "cronjob_type": "'once' or 'loop'.",
            "action": "str, e.g. 'do_this_later'.",
            "time_between_runs": "int, seconds.",
            "params": "dict (optional)."
        },
        returns = {
            "addr": "addr to coerse"
        }
        """,
        name="Goal.add_cron_job",
        
    )
)

# result_add_master_result
register(
    Executable(
        description="Append text to the master result.",
        body=Path(__file__) / "executable_files" / "result_add_master_result.py",
        header="""
        args = {
            "text": "str, text to append."
        },
        returns = ""
        """,
        name="Result.add_master_result",
        
    )
)

# result_write
register(
    Executable(
        description="Write the result of the current slave instruction.",
        body=Path(__file__) / "executable_files" / "result_write.py",
        header="""
        args = {
            "text": "str, result text."
        },
        returns = ""
        """,
        name="Result.write",
        
    )
)

# web_search_fulltext
register(
    Executable(
        description="Search web and return full text of top pages.",
        body=Path(__file__) / "executable_files" / "web_search_fulltext.py",
        header="""
        args = {
            "query": "str, search query.",
            "websites_amount": "int (optional, default 3)."
        },
        returns = {
            "full_result": "a very large string, XML delimetered... propably."
        }
        """,
        name="Web.search_fulltext",
        
    )
)

# web_search
register(
    Executable(
        description="Search web and return list of URLs with titles and snippets.",
        body=Path(__file__) / "executable_files" / "web_search.py",
        header="""
        args = {
            "query": "str, search query.",
            "amount_results": "int, number of results."
        },
        returns = [
            {
                "url": "string",
                "title": "string",
                "snippet": "string"
            },
            ...
        ]
        """,
        name="Web.search",
        
    )
)

# web_get
register(
    Executable(
        description="Perform HTTP GET request.",
        body=Path(__file__) / "executable_files" / "web_get.py",
        header="""
        args = {
            "url": "str, URL.",
            "timeout": "int (optional, default 10).",
            "return_type": "'extracted' or 'raw' (optional, default 'extracted').",
            "headers": "dict (optional)."
        },
        returns = {
            "content": "potentially large string"
        }
        """,
        name="Web.get",
        
    )
)

# web_post
register(
    Executable(
        description="Perform HTTP POST request.",
        body=Path(__file__) / "executable_files" / "web_post.py",
        header="""
        args = {
            "url": "str, URL.",
            "timeout": "int (optional, default 10).",
            "return_type": "'extracted', 'raw', or 'status_code' (optional, default 'extracted').",
            "headers": "dict (optional).",
            "payload": "str (optional)."
        },
        returns = {
            "content": "potentially giant string"
        }
        """,
        name="Web.post",
        
    )
)

# event_register_reaction_rmt
register(
    Executable(
        description="Register an RMT as reaction to an event (Event module).",
        body=Path(__file__) / "executable_files" / "event_register_reaction_rmt.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "rmt_id": "int or str, RMT address.",
            "args": "dict (optional)."
        },
        returns = {
            "consumer_addr": "consumer_addr to coerse"
        }
        """,
        name="Event.register_reaction_rmt",
        
    )
)

# event_register_reaction_slave
register(
    Executable(
        description="Register a slave as reaction to an event (Event module).",
        body=Path(__file__) / "executable_files" / "event_register_reaction_slave.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "instruction": "str, slave instruction.",
            "scope": "str (optional, default 'general')."
        },
        returns = {
            "consumer_addr": "consumer_addr to coerse"
        }
        """,
        name="Event.register_reaction_slave",
        
    )
)

# event_create_result
register(
    Executable(
        description="Create a result filled by an event (Event module).",
        body=Path(__file__) / "executable_files" / "event_create_result.py",
        header="""
        args = {
            "event_path": "str, NATS event subscription.",
            "result_str": "str, template with ${{data}} and ${{event}}.",
            "name": "str (optional)."
        },
        returns = {
            "result_addr": "addr to coerse",
            "consumer_addr": "addr to coerse"
        }
        """,
        name="Event.create_result",
        
    )
)

# report_report_paradoxal_information
register(
    Executable(
        description="Report paradoxical information (aborts execution).",
        body=Path(__file__) / "executable_files" / "report_report_paradoxal_information.py",
        # NOTE: The error is correctly propagated because syscall is executed in tool
        header="""
        args = {
            "items": "list of int/str, addresses or names of paradoxical items.",
            "paradox": "str, description of the paradox."
        },
        returns = ""
        """,
        name="Report.report_paradoxal_information",
    )
)
