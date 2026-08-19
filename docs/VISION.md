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

To explain this further, CPUs just read instructions and write results back.
They do a lot more in modern practice, but most of it is optimisations, core idea as described in Turing Mashine is just reading and writing data.

So the cores are basically that. They read instructions, then they execute the computation, and then they write back the results, and repeat. 

___

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

____

#### Anouther algoritm idea Navigatable Direction Vector [ ]:

NDV is anouther idea on how to get linear positions required for viewing window. 

The radical differense between these 2 is that Rainbow sort ultimately *attempts* to make an arbitrarily large dimentionality into 1 dimentional list, which at its core, is mathematically impossible to do accurately. 
So rainbow sort literaly boils down to being my practical UMAP attempt for this exact use case. 

Now, NDV doesnt even try to make a stable 1d index, it gets the 1d navigatable line by treating the 1d as Magnitude of an anchor vector.
Anchor vector is thus the target embedding. How exactly its gotten can be seen in the "semantic mathematics" section of the VISION.

We can define the line as all points in the given direction, e.g. all the products of all of $\mathbb{R}$ and a unit vector.
Anchor vector can be converted to unit vector like this: 
$ \hat{u} = \dfrac{\text{anchor\_vector}}{\lVert \text{anchor\_vector} \rVert}$.
So the line is:
 TODO : FINISH MATHS.

We can define the viewing window logically as the cylinder around the given segment of the line, encompasing all items withhin that cylinder, and assosiating them to a Scalar position on the line.

Then we literaly just define the viewing window mathematically as:

____

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

#### Recursive master templates [ ]

Recursion of master templates is allowed, but they have to be marked as recursive, which should simply be a boolean flag collumn in the definition.

Recrusive master templates slaves will be internally migrated to the internally derived scope "rec_{scope_name}".
That scope will include the "return" tool, as well as the instructions on how to use it.
The explanation of that will also be part of base knowledge.

The "return" tool will return the results of this recursive iteration as the results of the first iteration of this instantiation of the recursive RMT.
The tracing of that will be done via a metadata fields such as "created_by" of type ADDR on slaves and masters, or specifically for recursive RMTs, they will utilize the metadata field "first_iteration" in the master metadata as well as "rmt" field in the metadata.

This basically includes constructing a metadata store for the DAG that documents the runtime creation of the DAG.

#### The metadata traces structure [x]

Now how should the metadata traces be structured?
First of all, there are many "meta types" in the system, so speaking of them, a unified metadata structure would not work.

So for storing DAG metadata, I would use this structure:
**metadata_dag**
addr REF master, slave, PK
metadata JSONB DEFAULT '{}'::JSONB
timestamp TIMESTAMP DEFAULT NOW();

**REASONING**:
If I were to try to make it actually DB good way, I end up with an ENUM and an explosion of tables.
If I were to try to then optimise that explosion of tables cause most of them are the same thing, I end up with literaly a wide NULLABLE table in the end, and still an explosion of logic needed to reconstruct the data as its split across way to many places.

So insdead I siply do the JSONB metadata collumn to store all the attributes, which basically saves me the headache of trying to encode all of giant amounts of invariants into the DB structure. 

**To actually validate that input is correct through, I will write a bunch of CONSTRAINT CHECK on the metadata to ensure no shit happens.**

And what about other items such as knowledge, tools, and events/cronjobs ?
Well for them it will propably be the same, so I might as well make a "metadata" collumn just a full metadata table for everything.
But that creates the problem of having to make EVERY decision through ->> and not being able to just JOIN on the table has has things you need. 
So I would say the best way of handling it is to add more metadata tables, like metadata_tools, metadata_knowledge, metadata_views, etc. But they will all be the same structure.
Or I can make them the same table, and then PARTITION by the type? ........ from speed it will be the same, but I still like the design of splitting by the big types, so I think its better to do it this way... well I will decide later on.

I will also design many SQL functions and python functions for the interactions with the metadata JSONB to avoid raw access interveaning with the logic. TODO: Maybe apply this pattern to more parts of the codebase

I will also design many SQL functions and python functions for the interactions with the metadata JSONB to avoid raw access interveaning with the logic. TODO: Maybe apply this pattern to more parts of the codebase. 

##### Json structure of the traces on slaves [ ]
```json
{
    "executions": [
        {
            "tool_calls": [
                {
                    "name": "...",
                    "args": {...},
                    "error": "..." // Optional. This error was recovered inside of the execution.
                }
            ],
            "syscalls": [
                {
                    "name": "...",
                    "args": {...},
                    "error": "..." // mostly access errors since syscalls cant really just fail.
                },
            ],
            "error": "..." // Optional.
        },
    ]
}
```

**Implementation details**:
The tracing is implemented via the optimiser/tracing subsystem and has a file per traced area with hook functions that signal certain actions happenening.

____

### Event based proactivity [~]

Make an event recieving and reaction system, to allow the agent to proactively react to events.

This includes cronjobs system. TODO : Add rmt activation to cronjobs list.

The plan is to have a bunch of events sent into NATS.
And have event consumers listen for them.
NATS acts as a router.

This allows the writing of additional listeners in any language, and their potential registration.
TODO: Think of a protocol to allow event listeners to be written by the AI and registered dynamically by AI. This should be included in the tool rewrite.

___

### Create base state [x]

Create a framework for the devs to define the base state, the ground truth of the Enviroment, and not just the enviroment itself. That will be later used to move everything currently hardcoded into the DB, although it will still be hardcoded, but it will be better hard coded, cause its now uniform with anything AI itself writes, and no AI written tools will feel like Ad-Hoc, but rather all tools will look the same and have the same interfact.

___

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

##### Omni language tools [ ]
Design is pretty simple, we first define a language in the DB table "languages"
**languages**
addr
compiled BOOLEAN NOT NULL
extension TEXT NOT NULL
install_command BashText NOT NULL (Its OS dependant? Well ... write a tool that gives OS in return, and make AI figure it out. But what about built in languages? Python? Well in python it will be empty string cause python is there as dependancy of ALaDOS kernel.)
compilation_command BashText (Must take filename from stdin and output to the same file just with .bin extension.)
run_command BashText (Must take filename from stdin)
CHECK compilation_command if compiled else run_command.
package_list TEXT DEFAULT "" (package list that must be installed. Make a syscall for appending to it.)
enforce_package_existance_Command BashText NOT NULL (Must take in the package_list from stdin and work based on that.)

And from that specification its pretty clear how it would work. Support for different OS-es is pretty straitforward, we just assume Bash, if its powershell or cmd then ..... well AI sees the error, it corrects the error, and thats it. If it doesnt work cause launched from cmd? Well then I say to launch in WSL, windows is not officially supported.

#### Builtins access from subprocess [x]

Add anouther directory, so that ALaDOS.src.python was not the only python path.
Anouther python path would be ALaDOS.lib.system_call_name.

The sys calls would go through to the main ALaDOS process and will be executed via interrupts or via the tool execution function.

TODO: Implement the sys calls as interrupts for those that dont need slave or master id or stuff like that.

Transportation method: Event system.
    Extend base state to import the tooling of registering consumers,
        create the consumers with the custom callbacks,
        and then store the consumers.
    Extend the event system to take the stored custom consumers and simply run them.

___

### Fully move scopes into the DB [ ]

Make scopes fully DB defined.
Default scopes are defined via base state.

LRU cache the scope string, and make a helper to make resolve the scope string.
Invalidated on change of a tool in scope or of scope itself, checked DB side. 
Actual implementation the same way.

Scopes should include operations such as "calculate intersection %" and "merge".

___

### Making everything a DSL [ ]

The idea is that the model -> Enviroment interface is really imported, as proven many times by all harness research. To improve that in ALaDOS and for that aspect to match ALaDOS overall quallity, I plan to make most interactions go through Domain Specific languages, specifically a family of languages to be used in manipulating the enviroment better then just raw tool calls list.

The languages together will form a language family for manipulating ALaDOS enviroment, and they will be co designed alongside eachother to not conflics and not collide in meanings of symbols, in order to not confuse the LLM.

This kind of operations requires a comprehensive compiler and decompiler suite, which should use libraries for that, if possible, but target language is my own "Bytecode" in case of ALaDOS tools and in case of RMT, its actually a list os SQL commands or rows into the DB, so basically, I will have to hand write most back end myself, and the front end, e.g. lexer and parser, well it can be done by a library, but I will propably write it myself as well, since I kinda need to co design everything for everything to stay high quality. 

This also allows the optimiser to perform very good optimisations since it can analyse the usage patterns and auto create abstractions, since the system is designed in such a way that an audit of it is really easy. 
This also allows to give AI compiler errors over broken assumptions, like for example, function return types.
We can enforce function contracts with args and returns to be explicid, and then do TypeChecking and other general Semantic Analysis of the programm. It also allows the fundamental features like OOO execution to exist freely in the system on every layer of itm abd the optimizer to grasp and optimise every layer of the system.

#### Tools Language Specification [ ]
Base syntax:
```ALaDOS tools
var_name1 = "literal string"
var_name2 = {"literal": "json", "obj": ["e", "k", "t"]}
var_name3 = ["literal", "list", "of", "string"]
var_name4 = [{"literal": "list", "of": "json"}, {"obj": ["e", "k", "t", "s"]}]

tool_name{"arg_name": "literal_string_val", "arg_name_2": var_name1, "explanation": "invokes the tool once with this json object as arguments."}
tool_name[var_name_4] // invokes the tool once per element of array.
// Tools must take in json objects, that means invocation on array of string is not allowed!
tool_name{"arg_array": var_name4} // invokes tool with argument of type array.
tool_name[["literal", "array", "execution"]]
// top level tool execution without asignment just returns to the caller.

var = tool_name[
    anouther_tool_name[
        tool_name{
            "description": "arbitrary nesting allowed. All variables and nesting are optimised and rewritten anyways."
        }
    ]
]

(tool_with_side_effect{"param": "json"} tool_depending_on_side_effect{"params": "dont", "include": "the side effect."})
// () is a chain of actions, it basically means that these actions hard depend on eachother in a non obvious way.
// That construct forces them to be executed sequenatially, which avoids weird errors that arise due to Out Of Order execution of tools that implicidly depend on eachothers side effects.

tool_name {"args": "normal"} // This is valid.
val1="str" // this is valid
val2={"json": "object"} // This is valid.
val3="str" // Re Declaration of variables is not allowed. This raises SyntaxError. All variables must only be declared once. 

// BTW comments like this are not actually supported, for model reasoning, use <think></think> blocks, you can use as many of them as you want and insert them anywhere you want, they are removed before processing of your output anyways. 
```

##### Chain usage example
Suppose we have the tool "edit_knowledge", and we want to use it to edit a knowledge item.
Suppose knowledge item is:
**knowledge item**
*description* = "Awesome description"
*content* = "I am a sad little AI driver with no real skill."

Now we want to edit the "I am a" part into "Vibecoders are", and "driver" into "drivers".
Now, with actual tools you would simply write these 2 edits and execute them in a single call, but its not generalizably true with all operations.
So we suppose our "edit_knowledge" tool is inferiour to the good "K.edit" tool that supports many edits. (Wait it currently doesnt ... WUT DE FUCK? Added a TODO there.)

Okay, so avoiding that little confusion there, we write the change that we want like this:
```ALaDOS tools
edit_knowledge{"addr": "12345", "content_change": "<SEARCH></SEARCH>I am a<REPLACE>Vibecoders are</REPLACE>"}
edit_knowledge{"addr": "12345", "content_change": "<SEARCH></SEARCH>driver<REPLACE>drivers</REPLACE>"}
```
But now these are OOO executed ... and we get no error cause they dont depend on eachother ... I am terrible at making examples.

Anyways, wrapping them in a chain would execute them sequentially from top to bottom and from left to right, and that means that their implicid side effect dependancies are not explicid. Basically you have to make a chain if the tools depend on each others side effects, so nothing breaks.

Chained example:
```ALaDOS tools
(
edit_knowledge{"addr": "12345", "content_change": "<SEARCH></SEARCH>I am a<REPLACE>Vibecoders are</REPLACE>"}
edit_knowledge{"addr": "12345", "content_change": "<SEARCH></SEARCH>driver<REPLACE>drivers</REPLACE>"}
)
```
Now indenting is not relevant in this language, so you can actually do whatever you want with embeddings. Whitespace is relevant in telling where words end and start, but new lines arent. (Except when they are used as whitespace, then yeah, but you get the point, basically how C does it!)


##### Notes:

The design of anouther DAG layer below slaves offers us anouther paralization capability: The split between Semantic Cores, e.g. the cores that handle slaves, and the Virtual Mashine cores, that handle the actual tool calls. The Virtual Mashine cores can execute the AST top level nodes in paralel, since we know exactly what tools depend on each others outputs and side effects and which ones dont at parsing time.



#### RMT Language Specification [ ]

Base syntax:
```RMT DSL
node first_action {
    instruction = ""
    scope = ""
    window = ["", ""]
}

node mid_action_1 {
    instruction = ""
    scope = ""
    window = [""]
}

node mid_action_2 {
    instruction = ""
    scope = ""
    window = [""]
}

node mid_action_3 {
    instruction = ""
    scope = ""
    window = [""]
}

node final_1 {
    instruction = ""
    scope = ""
    window = [""]
}

node final_2 {
    instruction = ""
    scope = ""
    window = [""]
}

first_action -> mid_action_1 -> final_1
first_action -> mid_action_2 -> final_1
first_action -> mid_action_3 -> final_1
mid_action_1 -> final_2
mid_action_2 -> final_2
mid_action_3 -> final_2
```
name can not be node, that is invalid and will error.

The graph is constructed from references, which means that every usage of a name of a node is that node, and not a copy of it, and every declared node will be executed exactly once.

Window definition and rmt usage:
```RMT DSL
window create name {
    instruction = "Instruction to creating window"
    scope = ""
}

window referense name {
    addr = "12345"
    name = "ImportantWindow"
}

rmt rmt_id invoke as rmt_node_name with arguments {"json": "arguments", "for": {"the": "rmt"}}

node example {
    instruction = ""
}

rmt_node_name -> example
```
In window create, a temporary window is created and then deleted once the RMT finished executing.
This is acomplished using the temporary object registration, which is described in its respective section of this document.

In window alias, you declare an alias for an existing viewing window that is already registered in the DB, and that you want to use without changes.

The name or addr in window alias is the enviroment address or name.

One is required.

rmt invokations can be referensed as nodes in the graph composition, and behave like nodes in the graph.

##### Scoped Items

Also known as temporary items, are a construct planned for usage with RMTs, as well as in other parts, optionally. 
The specification is kinda like this:
**tmp_items**
addr REF addrs ON DELETE CASCADE ON UPDATE CASCADE
scope_addr REF results ON DELETE CASCADE ON UPDATE CASCADE

**TRIGGER AFTER INSERT/UPDATE ON tmp_items**
NOTIFY tmp_items_changed changed_item

And there is a **python listener**. (Or not even python, if we go the "hyperscale" route. But propably not needed, but really, it could be a subprocess. Or a python dedicated thread. But a custom listener is fine for now.)

Event listener does this: 
LISTEN tmp_items_changed
Keep track of them
LISTEN unblocked_result (add the trigger if it doesnt exist yet)

If unblocked_result in tmp_item.scope_addr:
    DELETE FROM addrs WHERE addr = tmp_item.addr

Use a dict to efficiently keep track of these items.

Metadata ... well We could keep record of these in metadata if wished for, it would be relatively easy.

___

### Make views into Items [ ]

Includes the making of more types of views, which will be described in the next section.

The viewing_window will be an object in DB.
viewing_window can be attached to a slave or a master.
viewing_windows dont propagate on recursive masters.

viewing_windows can be created deleted and modified. 
They will be subject to stricter concurrency controlls then simple OCC,
with possible cloning/branching paths. (More complex then MESI controlls, but DAG specific, closer to how compilers check use after free but use after destructive change/delete and not with indestructive changes. )

Also propably scoping and branching of the same viewing window, which means creating a copy of it to work on for yourself insdead of working on public version.

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

___

## Optimiser Meta learning [ ]

This is the culmination of the development process, the final piece on the road to an General Information Transofmer mashine, to the complete ALaDOS.

The optimiser consists of many "strategies" which it uses to optimise ALaDOS enviroment to lessen the friction of working with it.
That means creating abstractions over repetative work.

Optimiser has to track the changes, the most for the Strategy 4, and make sure to revert them if even more errors occur, to avoid degrading due to LLM fuckups.

### Strategy 1, RMT auto detector. [ ]

Because the entire backlog of the execution history is right there in the masters and slaves tables, all we need to do is detect repetative patterns and abstact that work away into an RMT.

#### Algoritm 
> [!IMPORTANT]
> This algoritm has to be improved in order to catch the pitfall of optimising already optimised stuff, e.g. it should be able to understand the metadata traces and exclude already implemented stuff.

> [!NOTE]
>  Any group size smaller then k (configurable) are removed.
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

> [!NOTE]
> This algoritm should also not try to unroll recusrive rmts.


#### Pitfalls: 
1. **RMTs activations need to be tracked**, and with the planned inline activation method, it will become harder. Else the optimiser will be trapped in a **loop of adding the same abstraction over and over again**. *This is partially addressed by the metadata system as part of recursive reusable master templates*.
2. The optimiser should not rely on the LLM to do all the work, it has to do the majorty of deterministic work, and let the LLM do the polish and integration work. 
3. Optimiser has to be fast, naive python loop wont work. Preferably due to operating on database structures, it should be written as plpgsql for the large deterministic pattern matching.


### Strategy 2, scope creation: [ ]

Check what tools were used together often, or searched for together, and combine them into scopes.
Check what scopes largely overlap and are used for the same tasks (by instruction emb similarity and actual overlap of elements.) and propose merging, then use the LLM to double check if the differense makes sense, and then merge if not.
Check tools that arent in any scopes, group them by description similarities using an algoritm adjesent to rainbow sort, and then propose thhat as new scope to the LLM evaluator. LLM can then describe the scope and name the scope as well as fine tune the scope or reject the scope.


### Strategy 3, knowledge caching: [ ]

Check for repeated web searches, and form the results of such web searches into knowledge items, timestamp them with the time, include the sources, and thats basically it.

### Strategy 4, reocurring error based function improvement: [ ]

Check for reocurring errors in the execution traces, and activate the improvement RMT.
Improvement RMT consists of the LLM deciding if the error is the bug in the function, or if its a description issue. If its description issue, it just fixes the description to match actual function behaviour, else it fixes the function.
___

## Presentation [ ]

Implement a good UI, and many UIs at that. Current webui is broken shit, and it will be replaced. 
The UI should be really really good for the system. 

Also show the traces of execution, itegrating the the metadata traces as implemented by *recursive rmts* section and then showing it in a clean UI, listing the entire DAG as clickable nodes with arrows and the metadata being shown.

Also show case the changes the Optimiser made to the system, as well as show metrics off.
