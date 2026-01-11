import json
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def carregar_lista_txt(path):
    """
    Carrega arquivo .txt (uma entrada por linha),
    removendo linhas vazias e comentários.
    """
    full_path = path

    if not os.path.isabs(path):
        full_path = os.path.join(BASE_DIR, path)

    with open(full_path, encoding="utf-8") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]
