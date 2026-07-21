#!/usr/bin/env python3

from typing import Callable, TypeAlias

from ..types import ReferenceTo
from ..utils.conn_factory import Conn

ItemLoader: TypeAlias = Callable[[ReferenceTo, Conn], str]

ITEMS_LOADERS: dict[str, ItemLoader] = {}

def register_item_loader(tablename: str):
    """
    The decorator to register the Item Loader for a given tablename.
    """
    def decorator(func: ItemLoader) -> ItemLoader:
        ITEMS_LOADERS[tablename] = func
        return func
    return decorator

def load_item(addr: ReferenceTo, tablename: str, conn: Conn) -> str:
    """ Dynamic load dispatcher for tablename and addr loading. """
    return ITEMS_LOADERS[tablename](addr, conn)
