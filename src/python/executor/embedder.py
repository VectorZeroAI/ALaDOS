#!/usr/bin/env python3
import threading

from ..utils.conn_factory import conn_factory
from ..utils.config_dir_resolver import config_dir_resolver
import tomllib
from .queue import embedder_queue
import httpx
from .types import api
from pydantic import TypeAdapter
from sentence_transformers import SentenceTransformer

config_dir = config_dir_resolver()
config_file_exe_f = config_dir / "executor.toml"
config_file_emb_f = config_dir / "embedder.toml"

config_file_exe = tomllib.loads(config_file_exe_f.read_text())
try:
    config_file_emb = tomllib.loads(config_file_emb_f.read_text())
except FileNotFoundError:
    print("embedder.toml not found. Defaulting to local and embedding cores = executor cores.")
    config_file_emb = {}

config_method = "local" if config_file_emb.get("api") is None else config_file_emb.get("api")
# config_file_emb api is a list of api objects, defined like this [[api]] in the file itself

api_validator = TypeAdapter(api)

embedder = SentenceTransformer("msmarco-distilbert-base-tas-b")

def setup():
    """ Set up the embeder threads """
    for _ in range(config_file_exe['cores_number']):
        threading.Thread(target=embedder_thread, daemon=True, args=(embedder_queue,)).start()

def embedder_thread():
    """ A single embeder thread object """

    def _local_emb_call(text: str):
        """ local embedder call """
        return embedder.encode(text)

    conn = conn_factory()

    while True:
        item_addr = embedder_queue.get_blocking()

        desc_and_type = conn.execute("""
    SELECT description, type FROM viewing_window vw INNER JOIN addrs_tables at ON at.addr = %s;
                 """, (item_addr,)).fetchone()
        if desc_and_type is None:
            print(f"Object that were supposed to embedd is not found. {item_addr} does not exist as an embeddable item.")
            continue

        if config_method == "local":
            emb = _local_emb_call(desc_and_type[0])
        else:
            for i in config_method: # pyright: ignore
                api_object = api_validator.validate_python(i)

                try:
                    emb = _call_jina_embedding(api_object, desc_and_type[0]) # pyright: ignore
                except Exception as e:
                    print(f"api embedding over this method {api_object} encoutered this error: {e}. Trying the next method.")
                    continue

                conn.execute(f"""
        UPDATE {desc_and_type[1]} SET emb = %s WHERE addr = %s;
                             """, (emb, item_addr)) # pyright: ignore
                break # The table name in there is SQL schema enforced to be only knowledge or executables, so I dont see a python whitelist nesesary.
            # The ignore is nesesary because it just says that fstring cannot be used as SQK, wich works at runtime, so I dont care.

def _call_jina_embedding(api: api, text: str):
    with httpx.Client(timeout=15) as client:
        response = client.post(
            url=api['url'],
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api['key']}"
                },
            json={
                "input": [text],
                "model": api['model'],
                "dimensions": 768,
                "task": "text-matching",
                "normalized": True
                }
            )
        response.raise_for_status()
        emb = response.json()['data'][0]
    return emb

