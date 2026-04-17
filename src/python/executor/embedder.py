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
    embedder = None

    def _local_llm_call(text: str):
        nonlocal embedder
        if not embedder:
            from sentense_transformers import SentenseTransformer
            embedder = 

    conn = conn_factory()
    while True:
        item_addr = embedder_queue.get_blocking()
        desc_and_type = conn.execute("""
    SELECT description, type FROM viewing_window vw INNER JOIN addrs_tables at ON at.addr = %s WHERE addr = %s;
                 """, (item_addr, item_addr)).fetchone()
        if config_method == "local":
            raise NotImplementedError("NOT IMPLEMENTED LOCAL EMBEDDING YET")
        else:
            for i in config_method:
                emb = _call_openai_embedding(i, desc[0])
                conn.execute(f"""
        UPDATE {desc[1]} SET emb = %s WHERE addr = %s;
                             """, (emb, item_addr))

def _call_openai_embedding(api: api, text: str):
    with httpx.Client(timeout=15) as client:
        response = client.post(
            url=api['url'],
            headers={
                "Content-Type": "application/json",
                "Autorization": f"Bearer {api['key']}"
                },
            json={
                "input": [text],
                "model": api['model'],
                "dimentions": 768,
                "task": "text-matching",
                "normalized": True
                }
            )
        response.raise_for_status()
        emb = response.json()['data'][0]
    return emb

