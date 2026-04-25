#!/usr/bin/env python3

from typing import TypeVar, Generic
import queue
import asyncio

T = TypeVar('T')

class Uqueue(Generic[T]):
    """
    Universal queue, my bloated abomination queue for everything ever.
    """
    def __init__(self):
        self._queue: queue.Queue[T] = queue.Queue()
        
    def put(self, item: T) -> None:
        """ Thread-safe, callable from anywhere """
        self._queue.put(item)
    
    async def get(self) -> T:
        """ Awaitable, non-blocking to the event loop """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._queue.get)
    
    def get_blocking(self) -> T:
        """ Escape hatch for non-async contexts """
        return self._queue.get()

    def get_all(self) -> list[T]:
        items = []
        while True:
            try:
                items.append(self._queue.get())
            except queue.Empty:
                break
        return items


