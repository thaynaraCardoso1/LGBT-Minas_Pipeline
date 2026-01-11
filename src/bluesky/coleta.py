import requests
import time
import json
from pathlib import Path

from src.utils.load_config import carregar_lista_txt
from src.utils.logger import setup_logger

LOGGER = setup_logger("logs/bluesky_coleta.log")

BASE_URL = "https://bsky.social/xrpc"

RAW_DIR = Path("bases/rede social/bluesky/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def login_bluesky(handle, password):
    """
    Autentica no Bluesky e retorna o accessJwt
    """
    url = f"{BASE_URL}/com.atproto.server.createSession"
    payload = {
        "identifier": handle,
        "password": password
    }

    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["accessJwt"]


def buscar_posts(jwt, query, limit=100, cursor=None):
    """
    Busca posts no Bluesky usando searchPosts
    """
    url = f"{BASE_URL}/app.bsky.feed.searchPosts"

    headers = {
        "Authorization": f"Bearer {jwt}"
    }

    params = {
        "q": query,
        "limit": limit
    }

    if cursor:
        params["cursor"] = cursor

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def texto_menciona_cidade(texto, cidades):
    texto = texto.lower()
    return any(cidade.lower() in texto for cidade in cidades)


def salvar_post(registro):
    output_file = RAW_DIR / "bluesky_raw.jsonl"
    with open(output_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


def coletar_bluesky():
    MAX_POSTS = 50
    total_salvos = 0

    cidades = carregar_lista_txt("configs/filtros/cidades_mg.txt")

    # queries amplas (não semânticas)
    queries = [
        "em",
        "cidade",
        "belo horixonte",
        "mg",
        "minas"
    ]

    with open("configs/tokens.txt", encoding="utf-8") as f:
        handle = f.readline().strip()
        password = f.readline().strip()

    jwt = login_bluesky(handle, password)
    LOGGER.info("Autenticado com sucesso no Bluesky")

    for q in queries:
        LOGGER.info(f"Buscando query ampla: '{q}'")
        cursor = None

        for _ in range(5):
            data = buscar_posts(jwt, q, cursor=cursor)

            posts = data.get("posts", [])
            if not posts:
                break

            for post in posts:
                texto = post["record"].get("text", "")

                if not texto:
                    continue

                if texto_menciona_cidade(texto, cidades):
                    salvar_post({
                        "platform": "bluesky",
                        "coleta_tipo": "cidade_only",
                        "query": q,
                        "author": post["author"]["handle"],
                        "created_at": post["record"].get("createdAt"),
                        "text": texto,
                        "raw": post
                    })

                    total_salvos += 1

                    if total_salvos >= MAX_POSTS:
                        LOGGER.info(f"Limite de {MAX_POSTS} posts atingido. Encerrando coleta.")
                        return

            cursor = data.get("cursor")
            if not cursor:
                break

            time.sleep(1.5)


if __name__ == "__main__":
    coletar_bluesky()
