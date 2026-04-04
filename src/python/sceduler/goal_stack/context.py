#!/usr/bin/env python3

from .types import SlaveObj, WindowDataRaw
from ...utils.conn_factory import conn_factory
from hilbertcurve.hilbertcurve import HilbertCurve
import umap
import numpy as np

def resolve_context(slave_obj: SlaveObj):
    conn = conn_factory()

    all_window_data = conn.execute("""
    SELECT window_position, window_size_r, window_size_l FROM master_context WHERE addr = $1;
                 """, (slave_obj['master_addr'],)).fetchall()

    all_load_data = conn.execute("""
    SELECT item_addr FROM master_load WHERE master_addr = $1;
                             """, (slave_obj['master_addr'],)).fetchall()



def resolve_window(window_data: WindowDataRaw):
    """ This function resolves a window from raw window data from the DB. It resolves to a list of addrs wich is the window itself """

    
def create_index():
    """ Creates and index and writes it to the DB. """
    p = 7

    conn = conn_factory()
    data = conn.execute("""
    SELECT emb, addr FROM viewing_window
                 """).fetchall()

    emd_addr_lists_tuple: tuple[list, list[int]] = ([], [])
    for i in data:
        emd_addr_lists_tuple[0].append(i[0])
        emd_addr_lists_tuple[1].append(i[1])
    
    reducer = umap.UMAP(random_state=42)
    points_2d = reducer.fit_transform(np.array(emd_addr_lists_tuple[0]))

    mins = points_2d.min(axis=0)
    maxs = points_2d.max(axis=0)

    scaled = (points_2d - mins) / (maxs - mins)
    int_points = (scaled * (2**p - 1)).astype(int)

    hc = HilbertCurve(p=p, n=2)
    hilbert_positions = np.array([hc.distance_from_point(pt.tolist()) for pt in int_points])

