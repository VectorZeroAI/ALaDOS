# Executor

The executor is the streamlined execution pipeline.
On initialisation, it takes in the ExecutorConfig.json from the alados config directory.
config structure
```json
{
    "api_keys": [
        {
            "api_key": "georugqwuo3rft9wuefiwzebgf",
            "endpoint": "http://endpoint.site/path"
        },
        {
            "api_key": "indefinetly repeatable",
            "endpoint": "indefinetly repeatable"
        },
    ],
    "number_of_cores": 12 // optional, defaults to as many cores as providers
}
```
Each core is basically an ThreadPoolExecutor that takes input slave goals from the queue, executes them and writes the result correspondingly.
Each core gets every api endpoint and credentials.
On api error, fallback to the next api endpoint and retry.
On tool call error: log the error, and launch anouther api call with a fallback prompt asking the AI to recover from the error.
On DB error: panic and interrupt everything until human intervention.

Note that even internal stuff goes through this executor system, although internal stuff gets inserted into the beginning of the queue, as to simulate higher priority.

## Tool Execution

The tool execution is fairly simple and straitforward.
The model outputs a json at the end of its Reasoning. That is the Acting part of the LLM call. 
That json is the following:
```json
{
    "tool_calls": [
        {
            "tool": "name",
            "arguments": ["args", "list"]
        },
        {
            "tool": "name2",
            "arguments": ["args2", "list2"]
        }
    ]
}
```

Each one of the tools gets executed with the arguments via looking the function up in a dict like this:

```python
def function(arg: Any) -> None:
    sleep(2)
    pass

FUNCTION["name"] = function

for i in json["tool_calls"]:
    try:
        FUNCTION[i["tool"]](i["arguments"])
    except Exception as e:
        log(e) # somehow
        # Trigger error recovery interrupt
```

> [!NOTE]
> I dont use Async coroutine, or Async green thread executor, because when a thread waits for the LLM to finish, it doesnt hammer the API with new calls, while an Async system would do that, else what is the actual differense? If I dont use await api_call , then yeah, it wont wait it and just to others and hammer the API to trigger ratelimits, but whats the point then? + python is actively trying to make threads truly paralel, removing GIL, so this is the best start in the long term.


### Tools list

1. K.sfind(querry)
2. K.write(addr, diff)
3. K.create_new(desc: str, content: str)
4. K.alias(addr, name)
5. K.read(addr)
6. G.read_addr(addr)
7. G.read_time(timestamp1, timestamp2) # Empty defaults to current timestamp
8. G.read_cause([list"", "of", "cause", "strs"])
9. goal.slave_insert(slave_goal_json, position: int)
10. goal.master_add(master_goal_json)
11. goal.slave_append(slave_goal_json)
12. web.get(URL)
13. web.get_with_cookie(URL, cookie_file_text_addr)
14. web.post(URL, payload_addr, headder)
15. web.post_with_cookies(URL, payload_addr, headder_addr, cookie_file_text_addr)
16. web.post_with_cookies_now(URL, payload_plaintext, headder_plaintext, cookie_file_text_addr)
17. context.load(addr)
18. context.window.land(squerry)
19. context.window.expand_l(amount_entries: int)
20. context.window.expand_r(amount_entries: int)
21. context.window.shrink_r(amount_entries: int)
22. context.window.shrink_l(amount_entries: int)
23. context.sload(squerry)
24. tool.sfind(semantic querry)
25. tool.find(querry)
26. tool.create_from_addr(addr)
27. tool.create_now(
        body="": str,
        headder="": str,
        desc="": str
    )
28. interrupt.invoke(ID)
