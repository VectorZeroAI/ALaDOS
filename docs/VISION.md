# VISION

This is the document describing my vision for the project.

## General direction

I plan for this project to head into the AGI research, and by that I mean that I belive LLMs are already good enough for them to be turned into AGI, all they need is a good enviroment, e.g. Harness. 
So I am building this AI Operating system, e.g. Operating system for AI, that will allow LLM to unlock its full capability and tacle extremely long horizont tasks and extremely large data natively.

## Architectural changes roadmap

### CPU like execution [x]

Have paralel cores that pull ReAct steps, execute them, and write the results back.
Include Concurrency controlls.
Have the cores be interruptable to do special instructions.
All work done through cores.

CPUs actually execute code using Out Of Order (OOO) execution, which means instructions execute in the order of their dependancy fullfillment, and not in the order they were written in, and that forms an execution DAG.

There are also instructions VS processes, so we will have ReAct steps and containers for them, e.g. slave goals and master goals.

This also means that there are no subagents allowed and no context boudaries allowed,
just like in the computer.
(OS does enforce memory boudaries but its for security, else there are none. Also for MESI and its optimisations kinda do memory boudaries, but there are fundamentally none, all that are are simply workarounds.)

### Viewing window context [x]

Dont use RAG, use viewing windows.
A viewing window is a navigatable slice of the shared knowledge plane.

Operations are "land", "move", "resize".

The viewing window ordering of items,
which is basically the requirement for such a structure to work,
is going to be my algoritm of "rainbow-ordering". I dont know if that algoritm already exists and has a name, but I will name it anyways. 

Rainbow order is the idea of "the most similar items are right and left of me, and the farther from me, the less similar the items become", like in a rainbow, if you take the colors, the nearest 2 to each color are its most similar colors.

This is achived by this algoritm:

First item position = 1.

Second item position = 2.

All next items, on insert placed into:
    if similarity of 2 most similar items greater then 0.4:
        2 most similar items positions summed / 2
    elif similarity of 1 of the items greater then 0.4:
        (The items position + MAX(position) + 1) / 2
    else: # No items are similar enough.
        MAX(position) + 1

> [!NOTE]
> All actual numbers are adjustable to future changes. Current numbers as of 2026-08-10 use + 100 on positions and 0.4 as threshhold. May change in the future, but core algoritm remains the same.

### Reusable Master Templates RMT [x]

Make master templates with keys that can be replaced at activation time.
Used for storing and reusing complex worksflows. 

Supports creation from scratch via DSL and atomic editing functions.

DSL format:
"""
START -> (instruction='text', scope='general', id='start') -> (instruction='text', scope='task', id='end') -> END
(id='start') -> (instruction='intermediate, and scope and id are optional. Defaults are "general", and a uuid.') -> (id='end')
(id='end') -> (instruction='appended. START and END dont do anything.')
"""

This kind of system would allow the agent to learn complex workflows as well as test and refine them, which is far better then the SKILL.md system, which is the alternative.
Or GemBots or whatever OpenAIs idea is. 

### Event based proactivity [~]

Make an event recieving and reaction system, to allow the agent to proactively react to events.

This includes cronjobs system. TODO : Add rmt activation to cronjobs list.

The plan is to have a bunch of events sent into NATS.
And have event consumers listen for them.
NATS acts as a router.

This allows the writing of additional listeners in any language, and their potential registration.
TODO: Think of a protocol to allow event listeners to be written by the AI and registered dynamically by AI. This should be included in the tool rewrite.


### Create base state [x]

Create a framework for the devs to define the base state, the ground truth of the Enviroment, and not just the enviroment itself. That will be later used to move everything currently hardcoded into the DB, although it will still be hardcoded, but it will be better hard coded, cause its now uniform with anything AI itself writes, and no AI written tools will feel like Ad-Hoc, but rather all tools will look the same and have the same interfact.

### Fully move tools into the DB [ ]

Use the base state to move the tools into the DB.
Refactor the tool execution flow to load tools from DB and execute them, e.g. uniformly ANY tools.

Use LRU cache with TRIGGER based invalidation.

For cache invalidation use a listener thread that puts in interrupts.
Also put more checkpoint() into core, its population was cut during core refactor.

Actual implementation pseudocode: 

TOOLS[name: str]: Callable[[], ActionConfirmation]
TOOLS_LOCK = threading.Lock()

interrupt_invalidate(tool_name: str):
    if tool_name in TOOLS:
        with TOOLS_LOCK:
            remove!

Normal case:
try:
    with TOOLS_LOCK:
        tool = TOOLS[name]
except CorrectExceptionForMissing:
    tool = build_tool(name)
    with TOOLS_LOCK:
        TOOLS[name] = tool

tool()

Optionally use bulk retrieval and then bulk execution, or one at a time looping,
its going to be slow as fuck because python anyways, so who cares.

Optionally TOOLS_LOCK can be refined into TOOLS_LOCK: dict[str, threading.Lock] for more atomic locks on tools for less blocking.

#### DB side tool system rewrite [ ]

Create protocol based handler registration. 
AI should be capable of creating a tool that follows a specific protocol, and to be able to register it as a new events emitter or a new format handler for the DataLoaders idea.

Tools that are part of protocolls are not to be invoked directly.

Tools should be allowed to be written in any language, via a "compiled" flag as well as compilation parameters. Will look into that in more details in the future. 

#### Builtins access from subprocess [ ]

Add anouther directory, so that ALaDOS.src.python was not the only python path.
Anouther python path would be ALaDOS.lib.system_call_name.

The sys calls would go through to the main ALaDOS process and will be executed via interrupts.

Transportation method: Event system.
    Extend base state to import the tooling of registering consumers,
        create the consumers with the custom callbacks,
        and then store the consumers.
    Extend the event system to take the stored custom consumers and simply run them.

### Fully move scopes into the DB [ ]

Make scopes fully DB defined.
Default scopes are defined via base state.

LRU cache the scope string, and make a helper to make resolve the scope string.
Invalidated on change of a tool in scope or of scope itself, checked DB side. 
Actual implementation the same way.

Scopes should include operations such as "calculate intersection %" and "merge".

### Make views into Items [ ]

Includes the making of more types of views, which will be described in the next section.

The viewing_window will be an object in DB.
viewing_window can be attached to a slave or a master.
viewing_windows dont propagate on recursive masters.

viewing_windows can be created deleted and modified. 
They will be subject to stricter concurrency controlls then simple OCC,
with possible cloning/branching paths. (More complex then MESI controlls, but DAG specific, closer to how compilers check use after free but use after destructive change/delete and not with indestructive changes. )

Also propably scoping and branching of the same viewing window, which means creating a copy of it to work on for yourself insdead of working on public version.

This also includes the rewrite of the rmt parser and serialiser into a new form for the new DSL speficiation.
New langauge syntax:
RMT usage:
{id='name of addr', args='{"json": "args", "allow": [{"full": "json"}, "speficiation", "."]}'}

Window definition:
window_name := (slave that makes it)
OR
window_name := {rmt that makes it}

Window attachment to slaves:

(instruction='Test', ..., window=['window_name', 'optionally many'])

Window attachment to masters and slaves in the builtins will be simply a new argument in there. 

Item Loads will be deprecated in favour of attaching a "comulative window"

### Viewing window types [ ]

More viewing window types includes basically the idea of filtering by type at the window creation time.

That means there will be viewing_window_tools, viewing_window_scopes, viewing_window_masters, etc.

This can also be used in preparation for the optimiser meta learning step.

This can also include NLP processing of knowledge items into more versions of the representations for different atomicity levels of viewing window access.
Kinda like NLP breaking all the knowledge items into statement triplest with attributes, and embedding and viewing over that pool.
Or even further processing into like Timelines and stuff, cause viewing windows can theoretically slide over any numerical collumn of the table, including timestamps.

This also includes the data loaders idea under the same "NLP processing suite" idea.

#### Cumulative windows [ ]

These viewing windows are different from normal ones.
They operate over data that the populating functions gave them in,
which means they fundamentally are already slices of total, e.g. they show a slice of the slice of total.
Which is different from normal viewing windows that show a slice of the whole Base.

#### Recursive windows [ ]

There are viewing windows of viewing windows, and as you guessed, yeah, recurse is allowed, e.g. viewing window of recursive viewing window of recusrive viewing windows of viewing windows kind of thingy. 

There should also be a cumulative version of this, for grouping viewing windows.

#### Data Loaders [ ]

The idea is to reuse the RLM idea of slicing large blobs of context up.

Basically the idea is the following: 
The user hands us a giant ass blob of data, like a 6 GB .db file or an 10GB XML or whatever else. 

The agent can then load it in using the DataLoaders type, and the DataLoader will process it into sortable, filterable, slidable parts. 
(optionally with embeddings. )

So the Agent can then querry the chunked, processed and indexed structure.
And with that ether populate an "cummulative context window" with querry results,
    or directly see the querry results,
    or even land a viewing window into the loaded data and explore it.

### Optimiser Meta learning [ ]

This is the culmination of the development process, the final piece on the road to an General Information Transofmer mashine, to the complete ALaDOS.

The optimiser consists of many "strategies" which it uses to optimise ALaDOS enviroment to lessen the friction of working with it.
That means creating abstractions over repetative work.

#### Strategy 1, RMT auto detector. [ ]

Because the entire backlog of the execution history is right there in the masters and slaves tables, all we need to do is detect repetative patterns and abstact that work away into an RMT.

##### Algoritm 

NOTE: Any group size smaller then k (configurable) are removed.
All instructions withhin members of the same slice (relative to start node graph position) must be highly similar. (90% or even higher similarity, e.g. "The same thing.")
All scopes must be exact same or have a very large intersection of eachover. (withhin slice)

Every n seconds (n can be large):
    Find slaves with instructions similar to eachother via embeddings
    for all the nodes in similar groups: 
        check indeg and outdeg, and check. 
        Group by matches.
        For all remaining groups:
            while group integrity holds for all members:
                Add a slice up and a slice down and check group integrity 
                (e.g. if conditions of grouping still hold.)
            The moment it doesnt: Backtract the side that it doesnt and continue.
            The moment both sides dont: Backtrack and treat the subgraphs where it holds as RMT candidates.
            Save rmt candidates and move on to next step.

    for all rmt candidates:
        Use rmt_from_range or a new custom made function to create the rmt out of them.
        Then serialise it. (At this point serialisation should be fixed and really good.)
        Then also serialise all of the subgraphs.
        Dump that all into context of LLM and say to fill out the instructions with input keys so that its reusable. 
        Deserialise back into DB.  (Check quality via anouther LLM Call beforehand.)
        Use an RMT call to make an LLM integrate the newly created rmt into the enviroment, e.g. adding it to scopes that seem to need it, by checking what scopes the slaves in there and upstream of that segment held. 

Thats approximately it.
Of course there may be better solutions, and I will look into them, but not now.


##### Pitfalls: 
1. RMTs activations need to be tracked, and with the planned inline activation method, it will become harder.Else the optimiser will be trapped in a loop of adding the same abstraction over and over and over and over.
2. The optimiser should not rely on the LLM to do all the work, it has to do the majorty of deterministic work, and let the LLM do the polish and integration work. 
3. Optimiser has to be fast, naive python loop wont work. Preferably in can even be written in a different language, like OCaml.


#### Strategy 2, scope creation [ ]

Check what tools were used together often, or searched for together, and combine them into scopes.
