import csv
import os
import json
import time
import zstandard as zstd

from .config import RAW_DIR, PROCESSED_DIR, carregar_config_reddit
from src.utils.logger import setup_logger
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto   # 👈 NOVO IMPORT


ARQUIVO_ZST = "RC_2025-04.zst"
CSV_SAIDA   = ARQUIVO_ZST.replace(".zst", "_BR.csv")


def extract_text(obj):
    if "body" in obj:
        return obj.get("body", "")
    else:
        return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")


def iter_zst(filepath, logger=None):
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as f:
        with dctx.stream_reader(f) as reader:
            buffer = ""
            total = 0

            while True:
                chunk = reader.read(2**20)
                if not chunk:
                    break

                text = chunk.decode("utf-8", errors="ignore")
                buffer += text
                linhas = buffer.split("\n")
                buffer = linhas[-1]

                for linha in linhas[:-1]:
                    total += 1

                    if logger and total % 100000 == 0:
                        logger.info(f"{total:,} linhas lidas...")

                    linha = linha.strip()
                    if not linha:
                        continue

                    try:
                        yield json.loads(linha)
                    except:
                        continue


def main():
    logger = setup_logger("logs/reddit_processamento.log")
    inicio = time.time()

    logger.info("==== INÍCIO DO PROCESSAMENTO ====")

    cfg = carregar_config_reddit()
    termos_lgbt   = cfg["termos_lgbt"]
    termos_odio   = cfg["termos_odio"]
    cidades_mg    = cfg["cidades_mg"]
    subreddits_br = cfg["subreddits_br"]

    caminho_zst = os.path.join(RAW_DIR, ARQUIVO_ZST)
    caminho_csv = os.path.join(PROCESSED_DIR, CSV_SAIDA)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    logger.info(f"📂 Lendo arquivo: {caminho_zst}")
    logger.info(f"💾 Salvando saída em: {caminho_csv}")

    # 👇 NOVOS CAMPOS
    campos = [
        "id",
        "author",
        "created_utc",
        "subreddit",
        "text_original",
        "text_clean",
        "has_lgbt_term",
        "has_hate_term",
        "has_mg_city",
    ]

    encontrados = 0

    with open(caminho_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        writer.writeheader()

        for obj in iter_zst(caminho_zst, logger):

            subreddit = (obj.get("subreddit") or "").lower()

            # ⭐ FILTRO 1: subreddit BR
            if subreddit not in subreddits_br:
                continue

            texto_original = extract_text(obj)
            texto_limpo = limpar_texto(texto_original)

            ok, matched_termos, matched_cidades = texto_casa_mg_lgbt(
                texto_limpo,
                termos_lgbt,
                termos_odio,
                cidades_mg
            )

            encontrados += 1

            writer.writerow({
                "id": obj.get("id"),
                "author": obj.get("author"),
                "created_utc": obj.get("created_utc"),
                "subreddit": obj.get("subreddit"),
                "text_original": texto_original,
                "text_clean": texto_limpo,
                "has_lgbt_term": int(any(t in matched_termos for t in termos_lgbt)),
                "has_hate_term": int(any(t in matched_termos for t in termos_odio)),
                "has_mg_city": int(bool(matched_cidades)),
            })

            if encontrados % 1000 == 0:
                logger.info(f"{encontrados} posts BR processados...")

    fim = time.time()
    logger.info(f"🎉 Total processado: {encontrados}")
    logger.info(f"⏱ Tempo total: {fim - inicio:.2f}s")
    logger.info("==== FIM DO PROCESSAMENTO ====")


if __name__ == "__main__":
    main()
