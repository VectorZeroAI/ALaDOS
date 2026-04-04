#!/usr/bin/env python3

from .types import SlaveObj, WindowDataRaw
from ...utils.conn_factory import conn_factory
from hilbertcurve.hilbertcurve import Hilbertcurve
import umap

def resolve_context(slave_obj: SlaveObj):
    conn = conn_factory()
    curr = conn.cursor()

    window_data = curr.execute("""
    SELECT window_position, window_size_r, window_size_l FROM master_context WHERE addr = ?;
                 """, (slave_obj['master_addr'],)).fetchall()

    load_data = curr.execute("""
    SELECT item_addr FROM master_loads WHERE master_addr = ?;
                             """, (slave_obj['master_addr'],)).fetchall()



def resolve_window(window_data: WindowDataRaw):
    curr = conn_factory().cursor()
    curr.execute("""""")
