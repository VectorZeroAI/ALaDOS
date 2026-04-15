#!/usr/bin/env python3
import threading

from ..utils.conn_factory import conn_factory
from ..utils.config_dir_resolver import config_dir_resolver
import tomllib
from .queue import embedder_queue
import httpx
from .types import api

config_dir = config_dir_resolver()
config_file_exe_f = config_dir / "executor.toml"
config_file_emb_f = config_dir / "embedder.toml"

config_file_exe = tomllib.loads(config_file_exe_f.read_text())
config_file_emb = tomllib.loads(config_file_emb_f.read_text())

config_method = "local" if config_file_emb.get("api") is None else config_file_emb.get("api")

def setup():
    for _ in range(config_file_exe['cores_number']):
        threading.Thread(target=embedder_thread, daemon=True, args=(embedder_queue,)).start()

def embedder_thread():
    conn = conn_factory()
    while True:
        item_addr = embedder_queue.get_blocking()
        desc = conn.execute("""
    SELECT description FROM viewing_window WHERE addr = %s;
                 """, (item_addr,)).fetchone()
        if config_method == "local":
            raise NotImplementedError("NOT IMPLEMENTED LOCAL EMBEDDING YET")
        else:
            for i in config_method:
                pass




def _call_openai_embedding(api: api):
    with httpx.Client(timeout=15) as client:
        client.post(
            url=api['url']

                )
