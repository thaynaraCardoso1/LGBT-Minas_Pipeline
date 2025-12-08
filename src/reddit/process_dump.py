import csv
import os
import json
import time
import zstandard as zstd
import pandas as pd

from .config import RAW_DIR, PROCESSED_DIR, carregar_config_reddit
from .filters import texto_casa_mg_lgbt
from src.utils.logger import setup_logger
from src.utils.lang.detector import is_portuguese


ARQUIVO_ZST = "RC_2025-05_comments.zst"
CSV_SAIDA   = ARQUIVO_ZST.replace(".zst", "_mg_lgbt.csv")


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
    termos_lgbt = cfg["termos_lgbt"]
    termos_odio = cfg["termos_odio"]
    cidades_mg  = cfg["cidades_mg"]

    caminho_zst = os.path.join(RAW_DIR, ARQUIVO_ZST)
    caminho_csv = os.path.join(PROCESSED_DIR, CSV_SAIDA)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    logger.info(f"📂 Lendo arquivo: {caminho_zst}")
    logger.info(f"💾 Salvando saída (incremental) em: {caminho_csv}")

    # adicionei matched_termos e matched_cidades no CSV pra você inspecionar depois
    campos = ["id", "author", "created_utc", "subreddit", "text", "matched_termos", "matched_cidades"]

    encontrados = 0

    with open(caminho_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        writer.writeheader()

        for obj in iter_zst(caminho_zst, logger):
            texto = extract_text(obj)

            # 1️⃣ Filtrar por idioma primeiro
            if not is_portuguese(texto):
                continue

            ok, matched_termos, matched_cidades = texto_casa_mg_lgbt(
                texto,
                termos_lgbt,
                termos_odio,
                cidades_mg,
            )

            if ok:
                encontrados += 1

                writer.writerow({
                    "id": obj.get("id"),
                    "author": obj.get("author"),
                    "created_utc": obj.get("created_utc"),
                    "subreddit": obj.get("subreddit"),
                    "text": texto,
                    "matched_termos": "|".join(matched_termos),
                    "matched_cidades": "|".join(matched_cidades),
                })

                if encontrados % 100 == 0:
                    logger.info(f"{encontrados} encontrados até agora.")

                # a cada 1000, mostra um exemplo de texto + quais termos/cidades bateram
                if encontrados % 1000 == 0:
                    logger.info("📝 Exemplo de match:\n" + texto[:400].replace("\n", " "))
                    logger.info(f"   → termos: {matched_termos}")
                    logger.info(f"   → cidades: {matched_cidades}")

    # se quiser normalizar created_utc depois:
    try:
        df = pd.read_csv(caminho_csv)
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", errors="coerce", utc=True)
        df.to_csv(caminho_csv, index=False, encoding="utf-8")
    except Exception as e:
        logger.warning(f"Não foi possível normalizar created_utc no final: {e}")

    fim = time.time()
    logger.info(f"Total filtrado: {encontrados}")
    logger.info(f"Tempo total: {fim - inicio:.2f}s")
    logger.info("==== FIM DO PROCESSAMENTO ====")


if __name__ == "__main__":
    main()
