# Task manager
Is basically a module exposing a bunch of methods.
The methods are the following:

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
