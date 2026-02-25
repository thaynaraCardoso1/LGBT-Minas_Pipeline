import csv
import os
import json
import logging
import io
import zstandard as zstd
from google.cloud import storage

from .config import PROCESSED_DIR, carregar_config_reddit
from src.utils.logger import setup_logger
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto

BUCKET_NAME = "lgbtminas-dados"
RAW_PREFIX = "rede social/raw/"
PROCESSED_PREFIX = "rede social/processed/"
CHECKPOINT_PREFIX = "rede social/tmp/"


def extract_text(obj):
    if "body" in obj:
        return obj.get("body", "")
    return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")


def list_zst_blobs(client):
    blobs = client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX)
    names = [b.name for b in blobs if b.name.endswith(".zst")]
    return sorted(names)


def read_checkpoint(client, checkpoint_blob):
    blob = client.bucket(BUCKET_NAME).blob(checkpoint_blob)
    if not blob.exists():
        return 0
    txt = blob.download_as_text().strip()
    return int(txt) if txt else 0


def write_checkpoint(client, checkpoint_blob, value):
    blob = client.bucket(BUCKET_NAME).blob(checkpoint_blob)
    blob.upload_from_string(str(value), content_type="text/plain")


def delete_blob_if_exists(client, blob_name):
    blob = client.bucket(BUCKET_NAME).blob(blob_name)
    if blob.exists():
        blob.delete()


def upload_file(client, local_path, dest_blob):
    blob = client.bucket(BUCKET_NAME).blob(dest_blob)
    blob.upload_from_filename(local_path)


def iter_zst_from_gcs(client, blob_name, skip_to=0, logger=None, filename=""):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)

    dctx = zstd.ZstdDecompressor()
    total_lidas = 0

    with blob.open("rb") as stream:
        with dctx.stream_reader(stream) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="ignore")
            for linha in text_stream:
                total_lidas += 1

                if total_lidas <= skip_to:
                    if logger and total_lidas % 2_000_000 == 0:
                        logger.info(f"[{filename}] ⏭️ Pulando: {total_lidas:,} linhas...")
                    continue

                if logger and total_lidas % 1_000_000 == 0:
                    logger.info(f"[{filename}] 📖 Lendo: {total_lidas:,} linhas...")

                linha = linha.strip()
                if not linha:
                    continue

                try:
                    yield json.loads(linha), total_lidas
                except Exception:
                    continue


def main():
    logger = setup_logger("logs/reddit_processamento_gcs.log")
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    logger.info("==== INÍCIO DA ESTEIRA (GCS STREAM) COM CHECKPOINT ====")

    client = storage.Client()
    cfg = carregar_config_reddit()
    subreddits_br = cfg["subreddits_br"]

    zst_blobs = list_zst_blobs(client)
    logger.info(f"Arquivos .zst encontrados no GCS: {len(zst_blobs)}")

    # ✅ MODO TESTE: processa só 1 arquivo (tira isso quando validar)
    zst_blobs = zst_blobs[:1]

    campos = [
        "id", "author", "created_utc", "subreddit",
        "text_original", "text_clean",
        "has_lgbt_term", "has_hate_term", "has_mg_city"
    ]

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for raw_blob in zst_blobs:
        nome_f = os.path.basename(raw_blob)

        local_csv = os.path.join(PROCESSED_DIR, nome_f.replace(".zst", "_BR.csv"))
        out_csv_blob = f"{PROCESSED_PREFIX}{nome_f.replace('.zst', '_BR.csv')}"
        checkpoint_blob = f"{CHECKPOINT_PREFIX}{nome_f.replace('.zst', '_checkpoint.txt')}"

        skip_to = read_checkpoint(client, checkpoint_blob)
        modo = "a" if skip_to > 0 else "w"

        if skip_to > 0:
            logger.info(f"[{nome_f}] ⚠️ Retomando da linha {skip_to:,} (checkpoint: gs://{BUCKET_NAME}/{checkpoint_blob})")
        else:
            logger.info(f"[{nome_f}] 🆕 Iniciando novo arquivo")

        encontrados = 0

        with open(local_csv, modo, newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=campos)
            if modo == "w":
                writer.writeheader()

            for obj, num_linha in iter_zst_from_gcs(client, raw_blob, skip_to, logger, nome_f):
                subreddit = (obj.get("subreddit") or "").lower()

                if subreddit in subreddits_br:
                    texto_original = extract_text(obj)
                    texto_limpo = limpar_texto(texto_original)

                    _, m_termos, m_cidades = texto_casa_mg_lgbt(
                        texto_limpo,
                        cfg["termos_lgbt"],
                        cfg["termos_odio"],
                        cfg["cidades_mg"]
                    )

                    encontrados += 1
                    writer.writerow({
                        "id": obj.get("id"),
                        "author": obj.get("author"),
                        "created_utc": obj.get("created_utc"),
                        "subreddit": obj.get("subreddit"),
                        "text_original": texto_original,
                        "text_clean": texto_limpo,
                        "has_lgbt_term": int(any(t in m_termos for t in cfg["termos_lgbt"])),
                        "has_hate_term": int(any(t in m_termos for t in cfg["termos_odio"])),
                        "has_mg_city": int(bool(m_cidades)),
                    })

                if num_linha % 100_000 == 0:
                    write_checkpoint(client, checkpoint_blob, num_linha)

        upload_file(client, local_csv, out_csv_blob)
        delete_blob_if_exists(client, checkpoint_blob)

        logger.info(f"[{nome_f}] ✅ Concluído! Total BR: {encontrados}. CSV -> gs://{BUCKET_NAME}/{out_csv_blob}")

    logger.info("🏁 ESTEIRA FINALIZADA!")


if __name__ == "__main__":
    main()
