#!/usr/bin/env python3
"""
Async client for reporting paradoxical information.
"""

from typing import Sequence

from .._.main import call


async def report_paradoxal_information(
    slave_addr: int,
    items: Sequence[int | str],
    paradox: str,
) -> None:
    """
    Reports that the given items contain mutually exclusive information.
    This will abort the current execution (raises ParadoxDetected on the slave).
    Returns nothing (the call never returns normally).
    """
    await call(
        "k_report_paradoxal_information",
        slave_addr,
        {"items": list(items), "paradox": paradox},
    )
