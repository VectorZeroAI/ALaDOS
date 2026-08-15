#!/usr/bin/env python3

from threading import RLock
from traceback import format_exception
from typing import Iterable, OrderedDict, Sequence, overload

from ..executor.types import Conn
from ..types import ReferenceTo, Singleton
from ..utils.conn_factory import conn_factory
from .logger import log_json


class NamesCacheManager(Singleton):
    """
    Caches the names address, so it doesnt need to be DB resolved again. 
    
    The order of LRU inform of OrderedDict is inverted,
    because default appention is to the last item, so the fist is the last appended.
    """
    def __init__(self, limit: int) -> None:
        self.cache = OrderedDict[str, ReferenceTo]()
        self.lock = RLock()
        self.conn = conn_factory()
        self.limit = limit
    


    @overload
    def __getitem__(self, key: str, /) -> int:
        ...

    @overload
    def __getitem__(self, key: tuple[str, ...], /) -> list[int]: 
        ...

    def __getitem__(self, key: str|tuple[str, ...], /) -> list[int]|int:
        coersed, rest = self.coearse(key)

        cache_hits, cache_misses = self.hit_cache(rest)

        new_addrs = self.batch_resolve_names(cache_misses)

        for a, n in zip(new_addrs, cache_misses, strict=True):
            with self.lock:
                self.cache[n] = a

            if len(self.cache) > self.limit:
                with self.lock:
                    for _ in range(len(self.cache) - self.limit):
                        self.cache.popitem(last=False)
        
        coersed.extend(cache_hits)
        coersed.extend(new_addrs)
        
        if len(cache_hits) == 1:
            return cache_hits[0] # This handles the -> int case.

        return cache_hits # This handles the -> list[int] case.
        

    def coearse(self, item: str|tuple[str, ...]) -> tuple[list[int], list[str]]:
        """
        Tries to coerse the string into an int,
        which is defined edge case and is supposed to be handled that way. 

        "120947" -> 120947
        """

        if isinstance(item, str):
            try:
                return ([int(item)], [])
            except ValueError:
                return ([], [item])
        
        coearsed: list[int] = []
        coearsed_worked: list[str] = []

        for n in item:
            try:
                coearsed.append(int(n))
                coearsed_worked.append(n)
            except ValueError:
                pass

        item_list = list(item)
        for i in range(len(item_list), 0, -1):
            if i in coearsed_worked:
                item_list.pop(i)

        return (coearsed, item_list)

            



    def invalidate(self, item: str|tuple[str, ...]) -> None:
        """ Invalidates the name. """
        if isinstance(item, str):
            if item in self.cache:
                with self.lock:
                    self.cache.pop(item)
                    return

        for n in item:
            with self.lock:
                if item in self.cache:
                    self.cache.pop(item)



    def batch_resolve_names(self, names: Iterable[str]) -> Iterable[int]:
        """
        Batch resolves all the cache misses efficiently.
        Bypasses resolve_name, insdead goes directly to source. 
        """
        addrs_fetch: list[tuple[int]] = self.conn.execute("""
        SELECT n.addr
        FROM unnest(%s) WITH ORDINALITY AS q(name, pos)
            JOIN names n ON q.name = n.name
        ORDER BY q.pos;
                          """, (names,)).fetchall()

        addrs: list[int] = [a[0] for a in addrs_fetch]
        return addrs



    def hit_cache(self, key: list[str]) -> tuple[list[int], list[str]]:
        """ Retrieves stuff for the key. Returns tuple (cache_hits, cache_misses). """
        results = ([], [])
        for k in key:
            with self.lock:
                if k in self.cache:
                    results[0].append(self.cache[k])
                else:
                    results[1].append(k)
        return results


names_cache_manager = NamesCacheManager(1000)


def resolve_to_addr(item: ReferenceTo|str) -> ReferenceTo:
    """
    Tries to resolve an items name if its name.
    Always returns address, raises RuntimeError if no address found.

    EDGE CASE:
        if its string coersed address, basically "183475203".
        
        This funcion DOES TRY TO COERSE, and the numeric only names are NOT ALLOWED.
        It is enforced at DB level that numbers only are not a valid name and are thus treated as address.
    """
    if isinstance(item, str):
        try:
            return names_cache_manager[item]
        except Exception as e:
            log_json({
                'type': 'util',
                'subtype': 'name_resolver',
                'status': 'fatal',
                'error': str(e),
                'traceback': str(format_exception(e))
            })
            raise RuntimeError(f"resolution failed due to {e}")

    return item
    

def resolve_to_addrs(names_and_addrs: Iterable[ReferenceTo|str]) -> list[ReferenceTo]:
    """
    Resolved the the strings of a list into the numeric addressess.
    Raises RuntimeError if a name couldnt be resolved.
    """

    to_resolve: list[str] = []
    were_addrs: list[int] = []
    for i in names_and_addrs:
        if isinstance(i, str):
            to_resolve.append(i)
        else:
            were_addrs.append(i)

    to_resolve_tuple: tuple[str, ...] = tuple(to_resolve)
    try:
        addrs = names_cache_manager[to_resolve_tuple]
    except Exception as e:
        log_json({
            'type': 'util',
            'subtype': 'name_resolver',
            'status': 'fatal',
            'error': str(e),
            'traceback': str(format_exception(e))
        })
        raise RuntimeError(f"Resolution failed with error {e}, because resolve_name somehow let an None through, or something was wrong upstream")
    
    addrs.extend(were_addrs)

    return addrs
    

def resolve_self(slave_addr: ReferenceTo, names_and_addrs: Sequence[str|ReferenceTo], conn: Conn) -> list[str|ReferenceTo]:
    """
    Resolved the "self" string in the input list to the slaves result address, slave address required.
    Later possibly will be expanded to include self_master as well.

    Resolves the slaves result addr automatically from the given addr.
    """
    result_addr = conn.execute_fetchval("SELECT result_addr FROM slaves WHERE addr = %s", (slave_addr,))

    names_and_addrs = list(names_and_addrs)
    for i in range(len(names_and_addrs)):
        if names_and_addrs[i] == "self":
            names_and_addrs[i] = result_addr
    
    return names_and_addrs
