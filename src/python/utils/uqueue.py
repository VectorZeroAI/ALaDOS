#!/usr/bin/env python3

from typing import TypeVar, Generic
from collections import deque
import asyncio

T = TypeVar('T')

class Uqueue(Generic[T]):
    """
    Universal queue, my bloated abomination queue for everything ever.
    """
    def __init__(self):
        self._queue: deque[T] = deque()
        
    def put(self, item: T) -> None:
        """ Thread-safe, callable from anywhere """
        self._queue.append(item)
    
    async def async_get(self) -> T:
        """ Awaitable, non-blocking to the event loop """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._queue.popleft)
    
    def get(self) -> T:
        """ Escape hatch for non-async contexts """
        return self._queue.popleft()

    def get_nowait(self) -> T|None:
        """ Returns an item if available, or None if no items. """
        try:
            return self._queue.popleft()
        except IndexError:
            return None

    def get_all(self) -> list[T]:
        items = []
        while self._queue:
            items.append(self._queue.popleft())
        return items

    def put_left(self, item: T) -> None:
        self._queue.appendleft(item)

    def __len__(self) -> int:
        return len(self._queue)


