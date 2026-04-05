# src/utils/filter_subreddits_gcs.py

import os
import csv
import time
import tempfile
import subprocess

from google.cloud import storage

from src.utils.logger import setup_logger


BUCKET = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")
PREFIX_BASE = os.getenv("TYBYRIA_PREFIX_BASE", "rede social")

PREFIXES = [
    f"{PREFIX_BASE}/processed/",
    f"{PREFIX_BASE}/analysis/vader/",
    f"{PREFIX_BASE}/analysis/tybyria/",
]

SUBREDDIT_COL = os.getenv("FILTER_SUBREDDIT_COL", "subreddit")
OUTPUT_SUFFIX = os.getenv("FILTER_OUTPUT_SUFFIX", "_mg_subreddits.csv")

TARGET_SUBREDDITS = {
    "minasgerais",
    "belohorizonte",
    "subredditsbrasil",
    "juizdefora",
    "uberlandia",
    "ouropreto",
    "uberaba",
    "montesclaros_",
}


def normalize_subreddit(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def list_csv_files(client: storage.Client, bucket: str, prefix: str):
    for blob in client.list_blobs(bucket, prefix=prefix):
        name = blob.name
        if not name.endswith(".csv"):
            continue
        if name.endswith(OUTPUT_SUFFIX):
            continue
        yield name


def output_blob_name(input_blob_name: str) -> str:
    base, _ = os.path.splitext(input_blob_name)
    return f"{base}{OUTPUT_SUFFIX}"


def already_done(client: storage.Client, bucket: str, blob_name: str) -> bool:
    return client.bucket(bucket).blob(blob_name).exists()


def filter_one_file(client: storage.Client, logger, input_blob_name: str):
    bucket = BUCKET
    out_blob_name = output_blob_name(input_blob_name)

    if already_done(client, bucket, out_blob_name):
        logger.info(f"✅ Já existe filtrado: gs://{bucket}/{out_blob_name} (skip)")
        return

    filename = os.path.basename(input_blob_name)
    logger.info(f"⬇️ Baixando: gs://{bucket}/{input_blob_name}")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_in = os.path.join(tmpdir, filename)
        local_out = os.path.join(
            tmpdir,
            os.path.splitext(filename)[0] + OUTPUT_SUFFIX
        )

        client.bucket(bucket).blob(input_blob_name).download_to_filename(local_in)

        total_rows = 0
        kept_rows = 0

        with open(local_in, "r", encoding="utf-8", newline="") as fin, open(local_out, "w", encoding="utf-8", newline="") as fout:
            reader = csv.DictReader(fin)

            fieldnames = reader.fieldnames or []
            if SUBREDDIT_COL not in fieldnames:
                raise ValueError(
                    f"Coluna '{SUBREDDIT_COL}' não existe em {filename}. Colunas encontradas: {fieldnames}"
                )

            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                total_rows += 1

                subreddit_value = normalize_subreddit(row.get(SUBREDDIT_COL, ""))

                if subreddit_value in TARGET_SUBREDDITS:
                    writer.writerow(row)
                    kept_rows += 1

        logger.info(
            f"[{filename}] ✅ Filtrado concluído. Linhas lidas: {total_rows:,} | Linhas mantidas: {kept_rows:,}"
        )

        gcs_out = f"gs://{bucket}/{out_blob_name}"
        cmd = ["gsutil", "-q", "cp", local_out, gcs_out]

        logger.info(f"☁️ Upload via gsutil: {' '.join(cmd)}")
        subprocess.check_call(cmd)

        logger.info(f"✅ Arquivo salvo: {gcs_out}")


def main():
    os.makedirs("logs", exist_ok=True)
    logger = setup_logger(f"logs/filter_subreddits_{time.strftime('%Y%m%d_%H%M')}.log")

    logger.info("==== INÍCIO - FILTRO DE SUBREDDITS (GCS) ====")
    logger.info(f"BUCKET={BUCKET}")
    logger.info(f"PREFIXES={PREFIXES}")
    logger.info(f"SUBREDDIT_COL={SUBREDDIT_COL}")
    logger.info(f"OUTPUT_SUFFIX={OUTPUT_SUFFIX}")
    logger.info(f"TARGET_SUBREDDITS={sorted(TARGET_SUBREDDITS)}")

    client = storage.Client()

    all_files = []
    for prefix in PREFIXES:
        files = list(list_csv_files(client, BUCKET, prefix))
        logger.info(f"🧾 Prefixo {prefix}: encontrados {len(files)} CSVs")
        all_files.extend(files)

    logger.info(f"🧾 Total de arquivos a avaliar: {len(all_files)}")

    for blob_name in sorted(all_files):
        try:
            filter_one_file(client, logger, blob_name)
        except Exception as e:
            logger.exception(f"❌ Falha em {blob_name}: {e}")
            continue

    logger.info("🏁 FIM.")


if __name__ == "__main__":
    main()
