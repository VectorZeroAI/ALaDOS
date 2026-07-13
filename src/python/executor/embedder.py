#!/usr/bin/env python3
import threading

from ..utils.config_handlers import load_apis, load_apis_from_text

from ..utils.conn_factory import conn_factory
from ..utils.config_dir_resolver import config_dir_resolver
import tomllib
from .queue import embedder_queue
import httpx
from .types import Api
from sentence_transformers import SentenceTransformer
from pgvector.psycopg import register_vector

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

embedder = SentenceTransformer("msmarco-distilbert-base-tas-b")

apis = load_apis_from_text(config_file_emb_f.read_text())

def setup():
    """ Set up the embeder threads """
    for _ in range(config_file_exe['cores_number']):
        threading.Thread(target=embedder_thread, daemon=True, ).start()
    conn = conn_factory()
    addrs = conn.execute("""
    SELECT addr FROM vector_ops WHERE emb IS NULL;
                 """).fetchall()
    for i in addrs:
        embedder_queue.put(i[0])


def embedder_thread():
    """ A single embeder thread object """

    def _local_emb_call(text: str):
        """ local embedder call """
        return embedder.encode(text)

    conn = conn_factory()
    register_vector(conn)


    while True:
        item_addr = embedder_queue.get()

        desc_and_type = conn.execute("""
    SELECT vp.description, vp.type FROM vector_ops vp WHERE vp.addr = %s
                 """, (item_addr,)).fetchone()
        if desc_and_type is None:
            print(f"Object that were supposed to embedd is not found. {item_addr} does not exist as an embeddable item.")
            continue

        if config_method == "local":
            emb = _local_emb_call(desc_and_type[0])
        else:
            for api in apis:
                try:
                    emb = _call_jina_embedding(api, desc_and_type[0]) # pyright: ignore
                except Exception as e:
                    print(f"api embedding over this method {api} encoutered this error: {e}. Trying the next method.")
                    continue
                break
            else: # This means all methods didnt work.
                print("falling back to local embeddings as API embeddings all failed spectacularly.")
                emb = _local_emb_call(desc_and_type[0])

        conn.execute("""
             UPDATE vector_ops SET emb = %s::vector(768) WHERE addr = %s;
                 """, (emb, item_addr))

def _call_jina_embedding(api: Api, text: str):
    with httpx.Client(timeout=15) as client:
        response = client.post(
            url=api.url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api.key}"
                },
            json={
                "input": [text],
                "model": api.model,
                "dimensions": 768,
                "task": "text-matching",
                "normalized": True
                }
            )
        response.raise_for_status()
        emb = response.json()['data'][0]
    return emb

