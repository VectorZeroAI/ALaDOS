#!/usr/bin/env python3
"""
Async client for goal‑oriented syscalls (slaves, masters, cron).
"""

from typing import Literal, Sequence

from .._.main import call
from ALaDOS.src.python.executor.types import SlaveScope # pyright: ignore


async def add_slave(
    slave_addr: int,
    instruction: str,
    slave_type: SlaveScope = "general",
    required_results_ids: Sequence[int | str] = [],
    slave_name: str | None = None,
    result_name: str | None = None,
) -> int:
    """
    Adds a new slave step to the current master.
    Returns the new slave's address.
    """
    result = await call(
        "goal_add_slave",
        slave_addr,
        {
            "instruction": instruction,
            "slave_type": slave_type,
            "required_results_ids": list(required_results_ids),
            "slave_name": slave_name,
            "result_name": result_name,
        },
    )
    return int(result)


async def add_planner_slave(slave_addr: int) -> None:
    """
    Adds a planner slave that will add further incremental steps.
    Returns nothing.
    """
    await call("goal_add_planner_slave", slave_addr, {})


async def add_master(
    slave_addr: int,
    instruction: str,
    required_ids: Sequence[int | str] = [],
    result_name: str | None = None,
) -> int:
    """
    Creates a new master goal.
    Returns the new master's address.
    """
    result = await call(
        "goal_add_master",
        slave_addr,
        {
            "instruction": instruction,
            "required_ids": list(required_ids),
            "result_name": result_name,
        },
    )
    return int(result)


async def add_cron_job(
    slave_addr: int,
    cronjob_type: Literal["once", "loop"],
    action: str,
    time_between_runs: int,
    params: dict[str, object],
) -> int:
    """
    Spawns a cron job.
    Returns the cron job's address.
    """
    result = await call(
        "goal_add_cron_job",
        slave_addr,
        {
            "cronjob_type": cronjob_type,
            "action": action,
            "time_between_runs": time_between_runs,
            "params": params,
        },
    )
    return int(result)
