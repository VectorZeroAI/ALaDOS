# Task manager
Exposes a bunch of methods for manipulating tasks, and keeps track of the Task context.

the methods are the following:

1. slave.insert(position relative to the currently executed slave goal, upwards only, slave_goal_json.)
2. master.add(master_goal_json)
3. master.decompose(master_goal_json)

## Explanations
#### slave.insert
inserts a slave goal. is callable by a tool.

#### master.add
Adds a master to the DB and then calls master.decompose on it.

#### master.decompose
Registeres a high priority slave goal to the beginning of the queue of the slave goals, with a prompt to create a list of slave goal json objects and return that as the result. Then LISTEN to the results arriving for the address of the result that is required. Get the json list of slave goal json objects from that result back, work through it, and insert all of that into the DB correctly.

### Notes
Slave goal json is:

```json
{
    "requirements": [67402, 29640], // list of integers
    "instruction": "", // instruction text
    "result_name": "name" // optional
}
```
Master goal json is:
```json
{
    "instruction": "", // instruction text
    "name": "name" // optional
}
```

## Task context tracker

The task manager keeps a bunch of data about the tasks context around.

Each task, e.g. master goal has a task shared context window. It consists of:
- sliding context window
- loaded addrs
- owned entires

That is managed by tools, wich are part of executor, not task manager. Task manager just loads them from the DB. 

### On startup 

The task manager constructs the semantically sorted list. 
It is constructed via UMAP ing the descriptions embeddings into 2D vectors, and then hilbert curve mapping them to a 1D list. 
The 1D list is the semantically sorted information base. 
