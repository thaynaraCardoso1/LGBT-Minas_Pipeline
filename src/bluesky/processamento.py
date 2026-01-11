import json
import pandas as pd
from pathlib import Path
import unicodedata
import re

from src.utils.limpeza import limpar_texto
from src.utils.logger import setup_logger
from src.utils.load_config import carregar_lista_txt

LOGGER = setup_logger("logs/bluesky_processamento.log")

RAW_FILE = Path("bases/rede social/bluesky/raw/bluesky_raw.jsonl")
PROCESSED_DIR = Path("bases/rede social/bluesky/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = PROCESSED_DIR / "bluesky_processed.csv"

CIDADES = carregar_lista_txt("configs/filtros/cidades_mg.txt")


def normalizar(texto: str) -> str:
    texto = texto.lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    texto = re.sub(r"[^a-z\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def detectar_cidade(texto: str, cidades: list[str]):
    texto_norm = normalizar(texto)

    for cidade in cidades:
        cidade_norm = normalizar(cidade)
        if cidade_norm in texto_norm:
            return cidade  # retorna a forma original do txt

    return None


def processar_bluesky():
    if not RAW_FILE.exists():
        LOGGER.error("Arquivo RAW não encontrado.")
        return

    registros = []

    with open(RAW_FILE, encoding="utf-8") as f:
        for line in f:
            post = json.loads(line)

            texto = post.get("text", "")
            texto_limpo = limpar_texto(texto)

            if not texto_limpo or len(texto_limpo) < 10:
                continue

            cidade_detectada = detectar_cidade(texto, CIDADES)

            if not cidade_detectada:
                continue  # filtro HARD por cidade

            registros.append({
                "platform": post.get("platform"),
                "coleta_tipo": post.get("coleta_tipo"),
                "query": post.get("query"),
                "author": post.get("author"),
                "created_at": post.get("created_at"),
                "cidade": cidade_detectada,
                "text": texto_limpo
            })

    if not registros:
        LOGGER.warning("Nenhum registro válido após processamento.")
        return

    df = pd.DataFrame(registros)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    LOGGER.info(f"{len(df)} registros salvos em {OUTPUT_CSV}")


if __name__ == "__main__":
    processar_bluesky()
