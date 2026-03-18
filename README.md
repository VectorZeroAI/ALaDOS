# ALaDOS

[A]rtificial [L]anguage [a]nd [D]isk [O]perating [S]ystem

This is an agentic system capable of continous self improvement, persistanse, Autonomus task execution, True Paralelism, and Out Of Order Execution.

## Architecture

The architecture is structured roughly like this:
1. DB 
2. Executor
3. Sceduler
4. Task Manager
5. Interrupt System
6. User interface
 
Each component is explained in its own section of the docs more precisely. 

## Overview

The workflow is something like this: 
1. user initialises it with a purpose
2. the system decomposes it into tasks
3. Each of the tasks is decomposed into ReAct steps
4. ReAct steps are executed in paralel.

Or this:
1. User pushes task 
2. its decomposed into ReAct steps
3. Steps are executed
4. User gets results


