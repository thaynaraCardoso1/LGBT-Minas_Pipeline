import csv
import os
import json
import time
import zstandard as zstd

from .config import RAW_DIR, PROCESSED_DIR, carregar_config_reddit
from src.utils.logger import setup_logger


ARQUIVO_ZST = "RS_2025-05_submissions.zst"
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
    subreddits_br = cfg["subreddits_br"]

    logger.info(f"🔍 Subreddits BR aceitos: {subreddits_br}")

    caminho_zst = os.path.join(RAW_DIR, ARQUIVO_ZST)
    caminho_csv = os.path.join(PROCESSED_DIR, CSV_SAIDA)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    logger.info(f"📂 Lendo arquivo: {caminho_zst}")
    logger.info(f"💾 Salvando saída em: {caminho_csv}")

    campos = ["id", "author", "created_utc", "subreddit", "text"]

    encontrados = 0

    with open(caminho_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        writer.writeheader()

        for obj in iter_zst(caminho_zst, logger):

            subreddit = (obj.get("subreddit") or "").lower()
            texto = extract_text(obj)

            # ⭐⭐ FILTRO ÚNICO: SUBREDDIT BR ⭐⭐
            if subreddit not in subreddits_br:
                continue

            encontrados += 1

            # Log de exemplo
            if encontrados <= 5:
                logger.info(f"\n📝 Exemplo {encontrados}:")
                logger.info(f"Subreddit: {subreddit}")
                logger.info(f"Texto    : {texto[:400].replace(chr(10),' ')}")

            writer.writerow({
                "id": obj.get("id"),
                "author": obj.get("author"),
                "created_utc": obj.get("created_utc"),
                "subreddit": obj.get("subreddit"),
                "text": texto,
            })

            if encontrados % 1000 == 0:
                logger.info(f"{encontrados} posts BR encontrados...")

    fim = time.time()
    logger.info(f"🎉 Total BR encontrados: {encontrados}")
    logger.info(f"⏱ Tempo total: {fim - inicio:.2f}s")
    logger.info("==== FIM DO PROCESSAMENTO ====")


if __name__ == "__main__":
    main()
