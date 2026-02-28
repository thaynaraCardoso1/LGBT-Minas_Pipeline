# src/analysis/vader_gcs.py
import os
import csv
import re
import time
import logging
import subprocess
from dataclasses import dataclass
from typing import Optional, Tuple

from google.cloud import storage
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Reaproveita teu logger, se quiser
from src.utils.logger import setup_logger

BUCKET = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")
PREFIX_BASE = os.getenv("TYBYRIA_PREFIX_BASE", "rede social")

PROCESSED_PREFIX = f"{PREFIX_BASE}/processed/"
TMP_PREFIX = f"{PREFIX_BASE}/tmp/"
OUT_PREFIX = os.getenv("VADER_OUT_PREFIX", f"{PREFIX_BASE}/analysis/vader/")

TEXT_COL = os.getenv("VADER_TEXT_COL", "text_clean")  # igual teu processed
ID_COL = os.getenv("VADER_ID_COL", "id")

CHECKPOINT_EVERY = int(os.getenv("VADER_CHECKPOINT_EVERY", "100000"))  # a cada 100k linhas
LOG_EVERY = int(os.getenv("VADER_LOG_EVERY", "1000000"))  # a cada 1M linhas


def gcs_read_checkpoint(client: storage.Client, bucket: str, ck_blob_path: str) -> int:
    b = client.bucket(bucket).blob(ck_blob_path)
    if not b.exists():
        return 0
    raw = b.download_as_text(encoding="utf-8").strip()
    try:
        return int(raw.replace(",", ""))
    except Exception:
        return 0


def gcs_write_checkpoint(client: storage.Client, bucket: str, ck_blob_path: str, value: int, logger: logging.Logger):
    b = client.bucket(bucket).blob(ck_blob_path)
    b.upload_from_string(str(value), content_type="text/plain")
    logger.info(f"☁️ Checkpoint enviado ao GCS: {ck_blob_path} = {value:,}")


def list_processed_files(client: storage.Client, bucket: str, prefix: str):
    # pega só *_BR.csv (ou o que você tiver)
    for blob in client.list_blobs(bucket, prefix=prefix):
        name = blob.name
        if name.endswith("_BR.csv"):
            yield name


def out_name_for(processed_blob_name: str) -> str:
    # Ex: rede social/processed/RC_2025-02_BR.csv -> RC_2025-02_BR_vader.csv
    base = processed_blob_name.split("/")[-1]
    return base.replace(".csv", "_vader.csv")


def already_done(client: storage.Client, bucket: str, out_blob_path: str) -> bool:
    return client.bucket(bucket).blob(out_blob_path).exists()


def run_one_file(
    client: storage.Client,
    logger: logging.Logger,
    processed_blob_path: str,
):
    bucket = BUCKET
    filename = processed_blob_path.split("/")[-1]
    out_blob_path = f"{OUT_PREFIX}{out_name_for(processed_blob_path)}"

    if already_done(client, bucket, out_blob_path):
        logger.info(f"✅ Já existe VADER: {out_blob_path} (skip)")
        return

    ck_blob_path = f"{TMP_PREFIX}{filename.replace('.csv','')}_vader_checkpoint.txt"
    skip_to = gcs_read_checkpoint(client, bucket, ck_blob_path)
    if skip_to > 0:
        logger.info(f"[{filename}] ⚠️ Retomando da linha {skip_to:,}")

    # Baixa o processed para /tmp (mais simples/robusto)
    local_in = f"/tmp/{filename}"
    local_out = f"/tmp/{filename.replace('.csv','')}_vader.csv"

    logger.info(f"⬇️ Baixando: gs://{bucket}/{processed_blob_path} -> {local_in}")
    client.bucket(bucket).blob(processed_blob_path).download_to_filename(local_in)

    analyzer = SentimentIntensityAnalyzer()

    processed_count = 0
    started_at = time.time()

    with open(local_in, "r", encoding="utf-8", newline="") as fin, open(local_out, "w", encoding="utf-8", newline="") as fout:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []
        if TEXT_COL not in fieldnames:
            raise ValueError(f"Coluna '{TEXT_COL}' não existe em {filename}. Colunas: {fieldnames}")

        extra_cols = ["vader_compound", "vader_pos", "vader_neu", "vader_neg"]
        writer = csv.DictWriter(fout, fieldnames=fieldnames + extra_cols)
        writer.writeheader()

        # pula linhas já processadas
        for _ in range(skip_to):
            try:
                next(reader)
            except StopIteration:
                break
            processed_count += 1

        for row in reader:
            txt = row.get(TEXT_COL, "") or ""
            scores = analyzer.polarity_scores(txt)

            row["vader_compound"] = scores["compound"]
            row["vader_pos"] = scores["pos"]
            row["vader_neu"] = scores["neu"]
            row["vader_neg"] = scores["neg"]

            writer.writerow(row)

            processed_count += 1
            if processed_count % CHECKPOINT_EVERY == 0:
                gcs_write_checkpoint(client, bucket, ck_blob_path, processed_count, logger)

            if processed_count % LOG_EVERY == 0:
                elapsed = time.time() - started_at
                logger.info(f"[{filename}] 📖 Processadas: {processed_count:,} linhas (elapsed {elapsed/60:.1f} min)")

    logger.info(f"[{filename}] ✅ VADER concluído. Linhas: {processed_count:,}")

    # Upload (via gsutil, igual você fez no outro pra evitar dor com resumable)
    gcs_out = f"gs://{bucket}/{out_blob_path}"
    cmd = ["gsutil", "-q", "cp", local_out, gcs_out]
    logger.info(f"☁️ Upload via gsutil: {' '.join(cmd)}")
    subprocess.check_call(cmd)

    # remove checkpoint quando finaliza
    client.bucket(bucket).blob(ck_blob_path).delete(if_generation_match=None)
    logger.info("🧹 Checkpoint removido (GCS).")

    # limpa arquivos locais
    try:
        os.remove(local_in)
        os.remove(local_out)
    except Exception:
        pass


def main():
    os.makedirs("logs", exist_ok=True)
    logger = setup_logger(f"logs/vader_{time.strftime('%Y%m%d_%H%M')}.log")
    logger.info("==== INÍCIO - VADER (GCS) - processar tudo do processed que faltar ====")
    logger.info(f"BUCKET={BUCKET} PROCESSED_PREFIX={PROCESSED_PREFIX} OUT_PREFIX={OUT_PREFIX}")

    client = storage.Client()
    files = list(list_processed_files(client, BUCKET, PROCESSED_PREFIX))
    logger.info(f"🧾 Encontrados {len(files)} arquivos processed *_BR.csv")

    for processed_blob_path in sorted(files):
        try:
            run_one_file(client, logger, processed_blob_path)
        except Exception as e:
            logger.exception(f"❌ Falha em {processed_blob_path}: {e}")
            # segue pro próximo
            continue

    logger.info("🏁 FIM.")


if __name__ == "__main__":
    main()
