import os
import sys
import traceback
import logging
from typing import Set, List

from google.cloud import storage

from src.utils.logger import setup_logger
from src.reddit.process_one_gcs import process_file_gcs


BUCKET = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")

# Prefixos no GCS
PREFIX_RAW = os.getenv("REDE_SOCIAL_RAW_PREFIX", "rede social/raw/")
PREFIX_PROCESSED = os.getenv("REDE_SOCIAL_PROCESSED_PREFIX", "rede social/processed/")
PREFIX_TMP = os.getenv("REDE_SOCIAL_TMP_PREFIX", "rede social/tmp/")

# Padrão de saída (compatível com o que você já gera: RC_YYYY-MM_BR.csv)
# Se quiser mudar o sufixo, altera aqui.
OUT_SUFFIX = os.getenv("PROCESSED_SUFFIX", "_BR.csv")

# Lista de arquivos para ignorar (separado por vírgula), ex:
# export EXCLUDE_RAW_FILES="RC_2025-01.zst,RC_2025-02.zst"
EXCLUDE_RAW_FILES = os.getenv("EXCLUDE_RAW_FILES", "").strip()

# Quantos arquivos processar no máximo (0 = todos)
MAX_FILES = int(os.getenv("MAX_FILES", "0"))

def parse_excludes(s: str) -> Set[str]:
    if not s:
        return set()
    return {x.strip() for x in s.split(",") if x.strip()}

def list_blobs(client: storage.Client, bucket_name: str, prefix: str) -> List[str]:
    bucket = client.bucket(bucket_name)
    return [b.name for b in client.list_blobs(bucket, prefix=prefix)]

def raw_to_processed_name(raw_blob_name: str) -> str:
    # raw:  rede social/raw/RC_2024-01.zst
    # proc: rede social/processed/RC_2024-01_BR.csv
    base = os.path.basename(raw_blob_name)  # RC_2024-01.zst
    if base.endswith(".zst"):
        base = base[:-4]
    return f"{PREFIX_PROCESSED}{base}{OUT_SUFFIX}"

def main():
    logger = setup_logger("logs/run_missing_raw_to_processed.log")
    for h in logger.handlers:
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.info("==== RUNNER: processar RAW que ainda não está em PROCESSED (GCS) ====")
    logger.info(f"Bucket: {BUCKET}")
    logger.info(f"RAW prefix: {PREFIX_RAW}")
    logger.info(f"PROCESSED prefix: {PREFIX_PROCESSED}")
    logger.info(f"TMP prefix: {PREFIX_TMP}")
    logger.info(f"OUT suffix: {OUT_SUFFIX}")

    excludes = parse_excludes(EXCLUDE_RAW_FILES)
    if excludes:
        logger.info(f"Excluindo arquivos RAW: {sorted(excludes)}")

    client = storage.Client()

    # Lista tudo que existe na RAW
    raw_blobs = [x for x in list_blobs(client, BUCKET, PREFIX_RAW) if x.endswith(".zst")]
    raw_blobs_sorted = sorted(raw_blobs)

    # Lista tudo que existe na PROCESSED
    processed_blobs = set(list_blobs(client, BUCKET, PREFIX_PROCESSED))

    logger.info(f"RAW .zst encontrados: {len(raw_blobs_sorted)}")
    logger.info(f"PROCESSED encontrados: {len(processed_blobs)}")

    # Descobre quais RAW estão faltando na PROCESSED
    to_process = []
    for raw_blob in raw_blobs_sorted:
        fname = os.path.basename(raw_blob)
        if fname in excludes:
            logger.warning(f"⏭️ Ignorado (exclude): {fname}")
            continue

        expected_processed = raw_to_processed_name(raw_blob)
        if expected_processed in processed_blobs:
            logger.info(f"✅ Já existe processed: {os.path.basename(expected_processed)} (skip)")
            continue

        to_process.append(raw_blob)

    logger.info(f"Arquivos para processar (RAW sem PROCESSED): {len(to_process)}")
    if MAX_FILES > 0:
        to_process = to_process[:MAX_FILES]
        logger.info(f"MAX_FILES aplicado -> vou processar só: {len(to_process)}")

    if not to_process:
        logger.info("Nada para processar. Encerrando.")
        return 0

    # Processa um por um
    for raw_blob in to_process:
        fname = os.path.basename(raw_blob)
        logger.info(f"➡️ Processando: {fname}")

        try:
            ok = process_file_gcs(
                bucket_name=BUCKET,
                raw_blob_path=raw_blob,              # já vem com prefix completo
                out_prefix_processed=PREFIX_PROCESSED,
                checkpoint_prefix=PREFIX_TMP,
                logger=logger,
            )
            if ok:
                logger.info(f"✅ Sucesso: {fname}")
            else:
                logger.warning(f"⚠️ Processou com ok=False: {fname} (seguindo pro próximo)")
        except Exception as e:
            logger.error(f"❌ Erro em {fname}: {e}")
            logger.error(traceback.format_exc())
            logger.info("➡️ Seguindo para o próximo arquivo...")

    logger.info("🏁 FIM: runner terminou.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
