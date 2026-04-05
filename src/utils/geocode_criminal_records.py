# src/criminal/geocode_criminal_gcs.py

import csv
import io
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

from google.cloud import storage
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
from geopy.geocoders import Nominatim


# =========================
# CONFIGURAÇÕES
# =========================
BUCKET_NAME = "lgbtminas-dados"
INPUT_BLOB = "criminal/raw/2026-02-23 - DIS - Registros - Eventos de LGBTQIAfobia - Jan 2016 a Jan 2025.csv"
OUTPUT_PREFIX = "criminal/processed"
LOG_DIR = "logs"

# nomes esperados das colunas
COL_BAIRRO = "Bairro"
COL_MUNICIPIO_COD = "Município (Código)"
COL_MUNICIPIO_FATO = "Município (Fato)"

# saída
OUT_LAT = "latitude"
OUT_LON = "longitude"

# geocoder
USER_AGENT = "lgbt-minas-geocoder/1.0"
SLEEP_BETWEEN_CALLS = 1.2
RETRIES = 3
TIMEOUT = 20


# =========================
# LOG
# =========================
def setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"geocode_criminal_{ts}.log")

    logger = logging.getLogger("geocode_criminal")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Log iniciado em %s", log_path)
    return logger


# =========================
# GCS
# =========================
def download_blob_text(client: storage.Client, bucket_name: str, blob_name: str) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text(encoding="latin1")


def upload_text_to_gcs(client: storage.Client, bucket_name: str, blob_name: str, text: str):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(text, content_type="text/csv; charset=latin1")


# =========================
# CSV
# =========================
def parse_csv(text: str):
    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    return reader.fieldnames, rows, delimiter


def validate_columns(fieldnames):
    required = [COL_BAIRRO, COL_MUNICIPIO_COD, COL_MUNICIPIO_FATO]
    missing = [c for c in required if c not in fieldnames]
    if missing:
        raise ValueError(f"Colunas obrigatórias não encontradas: {missing}")


# =========================
# GEO
# =========================
def normalize(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def geocode_with_retry(geolocator: Nominatim, query: str, logger: logging.Logger):
    for attempt in range(1, RETRIES + 1):
        try:
            location = geolocator.geocode(query, timeout=TIMEOUT)
            return location
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
            logger.warning("Tentativa %s falhou para query=%r: %s", attempt, query, e)
            time.sleep(2 * attempt)
    return None


def geocode_row(
    geolocator: Nominatim,
    bairro: str,
    municipio_fato: str,
    logger: logging.Logger,
    cache: Dict[str, Tuple[Optional[float], Optional[float]]]
) -> Tuple[Optional[float], Optional[float]]:
    bairro = normalize(bairro)
    municipio_fato = normalize(municipio_fato)

    if not municipio_fato:
        return None, None

    # 1) tenta bairro + município
    query1 = f"{bairro}, {municipio_fato}, MG, Brasil" if bairro else ""
    if query1:
        if query1 in cache:
            return cache[query1]

        loc = geocode_with_retry(geolocator, query1, logger)
        time.sleep(SLEEP_BETWEEN_CALLS)

        if loc:
            result = (loc.latitude, loc.longitude)
            cache[query1] = result
            return result

        cache[query1] = (None, None)

    # 2) fallback: município
    query2 = f"{municipio_fato}, MG, Brasil"
    if query2 in cache:
        return cache[query2]

    loc = geocode_with_retry(geolocator, query2, logger)
    time.sleep(SLEEP_BETWEEN_CALLS)

    if loc:
        result = (loc.latitude, loc.longitude)
        cache[query2] = result
        return result

    cache[query2] = (None, None)
    return None, None


# =========================
# MAIN
# =========================
def main():
    logger = setup_logger()
    client = storage.Client()

    logger.info("Lendo arquivo do bucket gs://%s/%s", BUCKET_NAME, INPUT_BLOB)
    text = download_blob_text(client, BUCKET_NAME, INPUT_BLOB)

    fieldnames, rows, delimiter = parse_csv(text)
    logger.info("Delimitador detectado: %r", delimiter)
    logger.info("Total de linhas lidas: %s", len(rows))
    logger.info("Colunas encontradas: %s", fieldnames)

    validate_columns(fieldnames)

    # mantém exatamente as colunas originais + latitude/longitude no final
    output_fields = list(fieldnames)
    if OUT_LAT not in output_fields:
        output_fields.append(OUT_LAT)
    if OUT_LON not in output_fields:
        output_fields.append(OUT_LON)

    geolocator = Nominatim(user_agent=USER_AGENT)
    cache: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    geocoded_ok = 0
    geocoded_empty = 0

    logger.info("Iniciando geocodificação...")

    for i, row in enumerate(rows, start=1):
        bairro = row.get(COL_BAIRRO, "")
        municipio_cod = row.get(COL_MUNICIPIO_COD, "")
        municipio_fato = row.get(COL_MUNICIPIO_FATO, "")

        lat, lon = geocode_row(
            geolocator=geolocator,
            bairro=bairro,
            municipio_fato=municipio_fato,
            logger=logger,
            cache=cache,
        )

        row[OUT_LAT] = lat if lat is not None else ""
        row[OUT_LON] = lon if lon is not None else ""

        if lat is not None and lon is not None:
            geocoded_ok += 1
        else:
            geocoded_empty += 1
            logger.warning(
                "Sem coordenadas | linha=%s | bairro=%r | municipio_cod=%r | municipio_fato=%r",
                i, bairro, municipio_cod, municipio_fato
            )

        if i % 100 == 0:
            logger.info(
                "Progresso: %s/%s | com coordenadas=%s | sem coordenadas=%s | cache=%s",
                i, len(rows), geocoded_ok, geocoded_empty, len(cache)
            )

    # escreve CSV em memória
    out_buffer = io.StringIO()
    writer = csv.DictWriter(
        out_buffer,
        fieldnames=output_fields,
        delimiter=delimiter,
        quoting=csv.QUOTE_MINIMAL,
        extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)

    input_filename = os.path.basename(INPUT_BLOB)
    output_filename = input_filename.rsplit(".", 1)[0] + "_geocoded.csv"
    output_blob = f"{OUTPUT_PREFIX}/{output_filename}"

    logger.info("Enviando resultado para gs://%s/%s", BUCKET_NAME, output_blob)
    upload_text_to_gcs(client, BUCKET_NAME, output_blob, out_buffer.getvalue())

    logger.info("Processo concluído.")
    logger.info("Total de linhas: %s", len(rows))
    logger.info("Com coordenadas: %s", geocoded_ok)
    logger.info("Sem coordenadas: %s", geocoded_empty)
    logger.info("Arquivo gerado: gs://%s/%s", BUCKET_NAME, output_blob)


if __name__ == "__main__":
    main()
