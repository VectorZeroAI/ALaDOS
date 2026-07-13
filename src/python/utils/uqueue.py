#!/usr/bin/env python3

import threading
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
        self._item_available = threading.Condition()
        
    def put(self, item: T) -> None:
        """ Normal put of a queue """
        with self._item_available:
            self._queue.append(item)
            self._item_available.notify()

    def prepend(self, item: T) -> None:
        with self._item_available:
            self._queue.appendleft(item)
            self._item_available.notify()

    async def async_get(self) -> T:
        """ Awaitable, non-blocking to the event loop """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get)
    
    def get(self) -> T:
        """ Waits for the item and gives you the item """
        with self._item_available:
            while not self._queue:
                self._item_available.wait()
            return self._queue.popleft()

    def get_nowait(self) -> T|None:
        """ Returns an item if available, or None if no items. """
        with self._item_available:
            try:
                return self._queue.popleft()
            except IndexError:
                return None

    def get_all(self) -> list[T]:
        items = []
        with self._item_available:
            while self._queue:
                items.append(self._queue.popleft())
        return items

    def get_end(self) -> T:
        """ Get the last item """
        with self._item_available:
            while not self._queue:
                self._item_available.wait()
            return self._queue.pop()

    def __len__(self) -> int:
        with self._item_available:
            return len(self._queue)

    def clear(self) -> None:
        """ Clears the queue without returning anything """
        self.get_all()
