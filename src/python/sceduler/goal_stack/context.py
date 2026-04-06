#!/usr/bin/env python3

from .types import SlaveObj, WindowData, LoadsData
from ...utils.conn_factory import conn_factory
from hilbertcurve.hilbertcurve import HilbertCurve
import umap
import numpy as np
from pydantic import TypeAdapter, ValidationError
from typing import Any


def resolve_context(slave_obj: SlaveObj):
    conn = conn_factory()

    window_data: Any = conn.execute("""
    SELECT window_anchor_exe, window_anchor_knowledge, window_size_r, window_size_l FROM master_context WHERE addr = $1;
                 """, (slave_obj['master_addr'],)).fetchone()

    window_data_python: WindowData = {
            "master_addr": slave_obj['master_addr'],
            "window_position": {
                "ref_addr": window_data[0] if window_data[0] is not None else window_data[1],
                "ref_table": "executables" if window_data[0] is not None else "knowledge",
                },
            "window_size_l": window_data[3],
            "window_size_r": window_data[2]
            }
    window_data_validator = TypeAdapter(WindowData)
    try:
        window_data_valid = window_data_validator.validate_python(window_data_python)
    except ValidationError as e:
        print(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")
        raise RuntimeError(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")

    window_context = resolve_window(window_data_valid)


    load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = $1;
                             """, (slave_obj['master_addr'],)).fetchall()

    loads_data_python: LoadsData = {
            "items_addrs": zip(*load_data),
            "master_addr": slave_obj["master_addr"]
        }
    loads_data_validator = TypeAdapter(LoadsData)
    try:
        loads_data_valid = loads_data_validator.validate_python(loads_data_python)
    except ValidationError as e:
        print(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")
        raise RuntimeError(f"context resolution failed, the context fetched from DB is: {window_data_python}, but validator says: {e}")

    load_context = resolve_loads(loads_data_valid)


def resolve_loads(loads_data: LoadsData) -> str
    """ Resolves loads raw data to context string """
    conn = conn_factory() # FIXME: make ruff ignore E703 in ruff config.

    all_items = []
    for addr in loads_data['items_addrs']:
        table = conn.execute("""
        SELECT table FROM addrs_tables WHERE addr = $1
                     """, (addr,)).fetchone()[0]

        match table:
            case 'knowledge':
                item = conn.execute("""
                    SELECT names.name, knowledge.content FROM knowledge WHERE addr = $1
                        JOIN names ON names.addr = $1;
                             """, (addr,)).fetchone()
            case 'executables':
                item = conn.execute("""
                    SELECT names.name, executables.header, executables.body FROM executables WHERE addr = $1
                        JOIN names ON names.addr = $1;
                                    """, (addr,)).fetchone()
            case 'logs':
                item = conn.execute("""
                    SELECT names.name, logs.created_at, logs.action, logs.created_by FROM logs WHERE addr = $1
                        JOIN names ON names.addr = $1;
                                    """, (addr,)).fetchone()

            case 'masters':
                slaves_fetch = conn.execute("""
                    SELECT instruction, result_addr, result_name FROM slaves WHERE master_addr = $1;
                                    """, (addr,)).fetchall()
                name = conn.execute("""
                    SELECT name FROM names WHERE addr = $1;
                                    """, (addr,)).fetchone()
                item = (name, slaves_fetch)

            case 'slaves':
                item = conn.execute("""
                    SELECT names.name,
                        slaves.master_addr,
                        slaves.instruction,
                        slaves.result_addr,
                        slaves.result_name
                        FROM slaves WHERE slaves.addr = $1
                            JOIN names ON names.addr = $1
                                    """, (addr,)).fetchone()
            case 'results':
                item = conn.execute("""
                SELECT s.result_name, 
                    r.content_str,
                    r.ready 
                    FROM results r WHERE r.addr = $1
                        JOIN slaves s ON s.result_addr = $1
                                    """, (addr,)).fetchone()
            case _:
                raise ValueError("Database returned a non existant or invalid table name. Returned {table}, but its not a valid table name. If it is, please add that tables case to the above handler.")
        
        all_items.append((item, table))
    return all_items
        




def resolve_window(window_data: WindowData) -> str:
    """ This function resolves a window from raw window data from the DB. It resolves to a context string. """
    conn = conn_factory()
    anchor_pos: Any = conn.execute("""
    SELECT position FROM $1 WHERE addr = $2
                 """, (
                     window_data["window_position"]["ref_table"],
                     window_data["window_position"]["ref_addr"]
                     )).fetchone()
    anchor_pos = int(anchor_pos[0])
    most_l_pos = anchor_pos - window_data['window_size_l']
    most_r_pos = anchor_pos - window_data['window_size_r']

    context_fetch = conn.execute("""
    SELECT description, addr, position FROM knowledge WHERE position BETWEEN $1 AND $2 ORDER BY position
    UNION ALL
    SELECT description, addr, position FROM executables WHERE position BETWEEN $1 AND $2 ORDER BY position;
                                 """, (most_l_pos, most_r_pos)).fetchall()

    descriptions, addrs, positions = zip(*context_fetch)

    names = []
    for a in addrs:
        names_fetch = conn.execute("""
        SELECT name FROM names WHERE addr = $1
                                   """, (a,)).fetchone()
        names.append(*names_fetch)

    
    context_str = ""
    for d, a, p, n in descriptions, addrs, positions, names:
        context_str = context_str + "@".join((n, f"pos: {p}", f"addr: {a}"))
        context_str = "\n".join((context_str, d, " ", " "))

    return context_str



    
def create_index():
    """ Creates an index and writes it to the DB. """
    p = 7

    conn = conn_factory()
    data = conn.execute("""
    SELECT emb, addr, type FROM viewing_window
                 """).fetchall()

    embs, addrs, types = zip(*data)

    reducer = umap.UMAP(random_state=42)
    points_2d: np.ndarray = reducer.fit_transform(np.array(embs)) # pyright: ignore

    mins = points_2d.min(axis=0)
    maxs = points_2d.max(axis=0)

    scaled = (points_2d - mins) / (maxs - mins)
    int_points = (scaled * (2**p - 1)).astype(int)

    hc = HilbertCurve(p=p, n=2)
    hilbert_positions = np.array([hc.distance_from_point(pt.tolist()) for pt in int_points])

    for pos, addr, t in zip(hilbert_positions, addrs, types):
        pos = int(pos)
        if t == "knowledge":
            conn.execute("""
            UPDATE knowledge SET position = $1 WHERE addr = $2;
                             """, (pos, addr))
        elif t == "executable":
            conn.execute("""
            UPDATE executables SET position = $1 WHERE addr = $2;
                         """, (pos, addr))
        else:
            raise ValueError(f"IDK WHAT HAPPENED THERE, but type gotten from the DB is {t}, wich is anything expected")

    conn.close() # NOTE : The connection is set to autocommit. 

def scrollable_index_thread():
    conn = conn_factory()
    conn.execute("LISTEN window_recreate")
    for n in conn.notifies():
        if n.channel != "window_recreate":
            continue
        create_intex()
