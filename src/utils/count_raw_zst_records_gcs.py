import os
import io
import csv
import time
from typing import Iterator, Tuple

import zstandard as zstd
from google.cloud import storage

from src.utils.logger import setup_logger


# =======================
# CONFIG
# =======================
BUCKET_NAME = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")
PREFIX = os.getenv("RAW_PREFIX", "rede social/raw/")
OUTPUT_DIR = "saida/raw_counts"
OUTPUT_CSV = "count_raw_zst_records.csv"
LOG_EVERY = int(os.getenv("RAW_COUNT_LOG_EVERY", "5000000"))
READ_SIZE = int(os.getenv("RAW_COUNT_READ_SIZE", str(8 * 1024 * 1024)))

RAW_INCLUDE_FILES = {
    "RC_2023-02.zst",
    "RC_2023-03.zst",
}

# =======================


def iter_raw_zst_blobs(client: storage.Client, bucket_name: str, prefix: str):
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        name = blob.name
        if name.endswith("/"):
            continue

        base = os.path.basename(name)

        if not (base.startswith("RC_") and base.endswith(".zst")):
            continue

        if RAW_INCLUDE_FILES and base not in RAW_INCLUDE_FILES:
            continue

        yield blob

def count_lines_in_zst_blob(blob, logger) -> int:
    """
    Conta registros em um .zst assumindo 1 JSON por linha.
    Faz streaming do blob + descompressão em stream.
    """
    line_count = 0
    chunk_count = 0
    last_byte = b""

    dctx = zstd.ZstdDecompressor(max_window_size=2147483648)

    started_at = time.time()

    with blob.open("rb") as compressed_f:
        with dctx.stream_reader(compressed_f) as reader:
            while True:
                chunk = reader.read(READ_SIZE)
                if not chunk:
                    break

                chunk_count += 1
                line_count += chunk.count(b"\n")
                last_byte = chunk[-1:]

                if line_count > 0 and line_count % LOG_EVERY < chunk.count(b"\n"):
                    elapsed = time.time() - started_at
                    logger.info(
                        f"[{blob.name}] 📖 {line_count:,} registros contados "
                        f"(elapsed {elapsed/60:.1f} min)"
                    )

    # se o arquivo não terminar com \n, conta a última linha
    if last_byte not in (b"", b"\n"):
        line_count += 1

    elapsed = time.time() - started_at
    logger.info(
        f"[{blob.name}] ✅ Concluído: {line_count:,} registros "
        f"(tempo {elapsed/60:.1f} min)"
    )

    return line_count


def main():
    os.makedirs("logs", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logger = setup_logger(f"logs/count_raw_zst_{time.strftime('%Y%m%d_%H%M')}.log")
    logger.info("==== INÍCIO - CONTAGEM DE REGISTROS RAW .ZST (GCS) ====")
    logger.info(f"BUCKET={BUCKET_NAME}")
    logger.info(f"PREFIX={PREFIX}")
    logger.info(f"READ_SIZE={READ_SIZE:,} bytes")
    logger.info(f"LOG_EVERY={LOG_EVERY:,}")

    client = storage.Client()
    blobs = list(iter_raw_zst_blobs(client, BUCKET_NAME, PREFIX))

    if not blobs:
        logger.warning(f"Nenhum arquivo RC_*.zst encontrado em gs://{BUCKET_NAME}/{PREFIX}")
        return 0

    logger.info(f"🧾 Encontrados {len(blobs)} arquivos .zst")

    rows = []
    grand_total = 0

    for blob in sorted(blobs, key=lambda b: b.name):
        try:
            logger.info(f"⬇️ Iniciando contagem: gs://{BUCKET_NAME}/{blob.name}")
            n = count_lines_in_zst_blob(blob, logger)
            rows.append({
                "arquivo": blob.name,
                "registros": n,
            })
            grand_total += n
        except Exception as e:
            logger.exception(f"❌ Falha em {blob.name}: {e}")
            rows.append({
                "arquivo": blob.name,
                "registros": "",
            })

    out_path = os.path.join(OUTPUT_DIR, OUTPUT_CSV)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["arquivo", "registros"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

        writer.writerow({"arquivo": "TOTAL_GERAL", "registros": grand_total})

    logger.info(f"📄 CSV gerado: {out_path}")
    logger.info(f"🏁 TOTAL GERAL: {grand_total:,} registros")
    logger.info("==== FIM ====")

    print(f"CSV gerado: {out_path}")
    print(f"TOTAL GERAL: {grand_total:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
