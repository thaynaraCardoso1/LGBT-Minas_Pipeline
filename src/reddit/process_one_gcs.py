import csv
import json
import os
import logging
import subprocess
import tempfile
from typing import Iterator, Tuple, Optional

import zstandard as zstd
from google.cloud import storage

from src.utils.logger import setup_logger
from src.reddit.config import carregar_config_reddit
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto


ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"  # 28 b5 2f fd


def extract_text(obj):
    if "body" in obj:
        return obj.get("body", "")
    return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")


def _read_checkpoint_gcs(client: storage.Client, bucket: str, checkpoint_blob_path: str) -> int:
    b = client.bucket(bucket).blob(checkpoint_blob_path)
    if not b.exists(client):
        return 0
    raw = b.download_as_text().strip()
    try:
        return int(raw.replace(",", ""))
    except:
        return 0


def _write_checkpoint_gcs(client: storage.Client, bucket: str, checkpoint_blob_path: str, value: int, logger):
    b = client.bucket(bucket).blob(checkpoint_blob_path)
    b.upload_from_string(str(value), content_type="text/plain")
    logger.info(f"☁️ Checkpoint enviado ao GCS: {checkpoint_blob_path} = {value:,}")


def _delete_blob_if_exists(client: storage.Client, bucket: str, blob_path: str, logger):
    b = client.bucket(bucket).blob(blob_path)
    if b.exists(client):
        b.delete()
        logger.info(f"🧹 Removido no GCS: {blob_path}")


def iter_zst_stream(reader, skip_to: int, logger, filename: str) -> Iterator[Tuple[dict, int]]:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(reader) as zr:
        buffer = ""
        total_lidas = 0

        while True:
            chunk = zr.read(2**20)  # 1MB
            if not chunk:
                break

            text = chunk.decode("utf-8", errors="ignore")
            buffer += text
            linhas = buffer.split("\n")
            buffer = linhas[-1]

            for linha in linhas[:-1]:
                total_lidas += 1

                if total_lidas <= skip_to:
                    if total_lidas % 2_000_000 == 0:
                        logger.info(f"[{filename}] ⏭️ Pulando: {total_lidas:,} linhas...")
                    continue

                if total_lidas % 1_000_000 == 0:
                    logger.info(f"[{filename}] 📖 Lendo: {total_lidas:,} linhas...")

                linha = linha.strip()
                if not linha:
                    continue

                try:
                    yield json.loads(linha), total_lidas
                except:
                    continue


def _is_valid_zst_magic(client: storage.Client, bucket: str, blob_path: str) -> bool:
    b = client.bucket(bucket).blob(blob_path)
    # baixa só os 4 primeiros bytes
    head = b.download_as_bytes(start=0, end=3)
    return head == ZSTD_MAGIC


def process_file_gcs(
    bucket_name: str,
    raw_blob_path: str,
    out_prefix_processed: str,
    checkpoint_prefix: str,
    logger: Optional[logging.Logger] = None,
    checkpoint_every: int = 100_000,
) -> bool:
    logger = logger or setup_logger("logs/process_one_gcs.log")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    raw_blob = bucket.blob(raw_blob_path)

    filename = os.path.basename(raw_blob_path)
    out_blob_path = f"{out_prefix_processed}{filename.replace('.zst', '_BR.csv')}"
    checkpoint_blob_path = f"{checkpoint_prefix}{filename.replace('.zst', '_checkpoint.txt')}"

    if not raw_blob.exists(client):
        logger.error(f"❌ Não existe no GCS: gs://{bucket_name}/{raw_blob_path}")
        return False

    # valida magic header (pula corrompido tipo 01 que começa com 00 00 00...)
    if not _is_valid_zst_magic(client, bucket_name, raw_blob_path):
        logger.error(f"❌ Arquivo não parece .zst válido (magic header inválido): {filename}")
        return False

    cfg = carregar_config_reddit()
    subreddits_br = set(cfg["subreddits_br"])

    skip_to = _read_checkpoint_gcs(client, bucket_name, checkpoint_blob_path)
    if skip_to:
        logger.info(f"[{filename}] ⚠️ Retomando da linha {skip_to:,}")
    else:
        logger.info(f"[{filename}] 🆕 Iniciando do zero")

    campos = [
        "id", "author", "created_utc", "subreddit",
        "text_original", "text_clean",
        "has_lgbt_term", "has_hate_term", "has_mg_city"
    ]

    encontrados = 0

    # grava local em /tmp e depois sobe via gsutil (mais estável que writer direto no GCS)
    tmp_out = os.path.join(tempfile.gettempdir(), filename.replace(".zst", "_BR.csv"))

    mode = "a" if (skip_to > 0 and os.path.exists(tmp_out)) else "w"

    with raw_blob.open("rb") as gcs_in, open(tmp_out, mode, newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        if mode == "w":
            writer.writeheader()

        for obj, num_linha in iter_zst_stream(gcs_in, skip_to=skip_to, logger=logger, filename=filename):
            subreddit = (obj.get("subreddit") or "").lower()

            if subreddit in subreddits_br:
                texto_original = extract_text(obj)
                texto_limpo = limpar_texto(texto_original)

                _, m_termos, m_cidades = texto_casa_mg_lgbt(
                    texto_limpo,
                    cfg["termos_lgbt"],
                    cfg["termos_odio"],
                    cfg["cidades_mg"],
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

            if num_linha % checkpoint_every == 0:
                _write_checkpoint_gcs(client, bucket_name, checkpoint_blob_path, num_linha, logger)

    logger.info(f"[{filename}] ✅ Processamento local concluído. Total BR: {encontrados:,}")

    # upload via gsutil (usa as permissões que você já confirmou que funcionam)
    dest = f"gs://{bucket_name}/{out_blob_path}"
    cmd = ["gsutil", "-q", "cp", tmp_out, dest]
    logger.info(f"☁️ Upload via gsutil: {' '.join(cmd)}")

    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        logger.error(f"❌ Falha no upload gsutil (code={r.returncode})")
        logger.error(r.stderr.strip() or r.stdout.strip())
        return False

    # se subiu, remove checkpoint
    _delete_blob_if_exists(client, bucket_name, checkpoint_blob_path, logger)
    logger.info("🏁 FIM.")
    return True


def main():
    logger = setup_logger("logs/process_one_gcs.log")
    for h in logger.handlers:
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.info("Use este módulo via import process_file_gcs().")


if __name__ == "__main__":
    main()
