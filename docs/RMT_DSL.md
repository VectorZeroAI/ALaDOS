# Reusable Master Templates Domain Specific Language Spec

The language looks the following:
~~~ RMT expression
START -> (task='task_text', id='lol1212123'") -> (task='task_text', id='anything really') -> (task='task_text') -> (task='task_text') -> (task='task_text') -> (task='task_text', id='stuff') -> END
~~~

each task, e.g. each block of text inside of (), gets an numeric incremental id starting at 1, or an id specified like above. 

Then, more paralel steps can be specified like this: 

~~~RMT expression
START -> (task="search the internet for {placeholder_name}", id="search") -> (task="synthesize the results on the question {question}", id="synthesis") -> END
START -> (task="search the internet for {placeholder_name2}", id="search2") -> ("synthesis")
~~~

## Syntax rules:

`START` specifies the begging of the DAG
`END` specifies the end of the DAG

`()` specifies a node
    `(task="str")` specifies a new node with the task.
    `(task="str", id="str")` specifies a new node with the task and the id, to referense the node.
    `("id_of_anouther_node")` specifies a referense to an already created node with the id.

Specification of START is and END is not strictly required, although its recommeded for clarity.
