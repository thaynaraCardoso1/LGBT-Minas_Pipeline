import io
import os
import sys
import csv
import logging
from datetime import datetime

import pandas as pd
import folium
from folium.plugins import MarkerCluster
from google.cloud import storage

# =========================
# CONFIG
# =========================
BUCKET_NAME = "lgbtminas-dados"
INPUT_BLOB = "criminal/processed/2026-02-23 - DIS - Registros - Eventos de LGBTQIAfobia - Jan 2016 a Jan 2025_geocoded.csv"
OUTPUT_BLOB = "criminal/maps/mapa_criminal_cluster.html"
LOG_DIR = "logs"

LAT_COL = "latitude"
LON_COL = "longitude"


# =========================
# LOG
# =========================
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"plot_criminal_map_{ts}.log")

    logger = logging.getLogger("plot_criminal_map")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Log iniciado: %s", log_path)
    return logger


# =========================
# GCS
# =========================
def download_blob_as_text(client, bucket_name, blob_name, logger):
    logger.info("Baixando gs://%s/%s", bucket_name, blob_name)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    data = blob.download_as_bytes()

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("Arquivo não é UTF-8, tentando latin1")
        return data.decode("latin1")


def upload_text_to_gcs(client, bucket_name, blob_name, text, logger, content_type="text/html"):
    logger.info("Enviando para gs://%s/%s", bucket_name, blob_name)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(text, content_type=content_type)


# =========================
# CSV
# =========================
def read_csv_from_text(text, logger):
    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","

    logger.info("Delimitador detectado: %r", delimiter)
    df = pd.read_csv(io.StringIO(text), delimiter=delimiter, encoding="latin1")

    # normaliza nomes das colunas
    df.columns = (
        df.columns
        .str.replace("\xad", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    # remove colunas vazias tipo Unnamed
    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed:")]
    if unnamed_cols:
        logger.info("Removendo colunas vazias extras: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    return df


# =========================
# MAPA
# =========================
def build_popup(row):
    partes = []
    for col in row.index:
        if pd.notna(row[col]) and str(row[col]).strip() != "":
            partes.append(f"<b>{col}</b>: {row[col]}")
    return "<br>".join(partes)


def normalizar_coord(valor):
    if pd.isna(valor):
        return None

    s = str(valor).strip()
    if not s:
        return None

    s = s.replace(" ", "")

    # tenta converter direto
    try:
        return float(s)
    except Exception:
        pass

    # corrige casos tipo -218.474.746 -> -21.8474746
    negativo = s.startswith("-")
    s_limpo = s.replace("-", "").replace(".", "").replace(",", "")

    if not s_limpo.isdigit() or len(s_limpo) < 3:
        return None

    s_corrigido = s_limpo[:2] + "." + s_limpo[2:]
    v = float(s_corrigido)

    if negativo:
        v = -v

    return v


def create_cluster_map(df, logger):
    df = df.copy()

    total_inicial = len(df)
    logger.info("Total de registros no arquivo: %s", total_inicial)

    df[LAT_COL] = df[LAT_COL].apply(normalizar_coord)
    df[LON_COL] = df[LON_COL].apply(normalizar_coord)

    logger.info("Após normalização numérica: %s linhas", len(df))

    df = df.dropna(subset=[LAT_COL, LON_COL])
    logger.info("Após dropna lat/lon: %s linhas", len(df))

    # filtro geográfico aproximado para MG
    df = df[
        df[LAT_COL].between(-25, -13) &
        df[LON_COL].between(-51, -39)
    ]

    logger.info("Após filtro geográfico MG: %s linhas", len(df))
    logger.info("Total descartado: %s", total_inicial - len(df))

    if df.empty:
        raise ValueError("Nenhum registro com latitude/longitude válido foi encontrado.")

    centro_lat = df[LAT_COL].mean()
    centro_lon = df[LON_COL].mean()

    mapa = folium.Map(
        location=[centro_lat, centro_lon],
        zoom_start=6,
        tiles="CartoDB positron"
    )

    marker_cluster = MarkerCluster(
        name="Registros agrupados",
        overlay=True,
        control=True
    ).add_to(mapa)

    for _, row in df.iterrows():
        popup_html = build_popup(row)

        folium.CircleMarker(
            location=[row[LAT_COL], row[LON_COL]],
            radius=4,
            popup=folium.Popup(popup_html, max_width=350),
            fill=True,
            fill_opacity=0.7,
            weight=1
        ).add_to(marker_cluster)

    folium.LayerControl().add_to(mapa)
    return mapa, len(df)


# =========================
# MAIN
# =========================
def main():
    logger = setup_logger()
    client = storage.Client()

    text = download_blob_as_text(client, BUCKET_NAME, INPUT_BLOB, logger)
    df = read_csv_from_text(text, logger)

    if LAT_COL not in df.columns or LON_COL not in df.columns:
        raise ValueError(
            f"O arquivo precisa ter as colunas '{LAT_COL}' e '{LON_COL}'. Colunas encontradas: {list(df.columns)}"
        )

    mapa, total_plotado = create_cluster_map(df, logger)
    html = mapa.get_root().render()

    upload_text_to_gcs(
        client=client,
        bucket_name=BUCKET_NAME,
        blob_name=OUTPUT_BLOB,
        text=html,
        logger=logger,
        content_type="text/html"
    )

    logger.info("Mapa gerado com sucesso.")
    logger.info("Total de pontos plotados: %s", total_plotado)
    logger.info("Arquivo final: gs://%s/%s", BUCKET_NAME, OUTPUT_BLOB)


if __name__ == "__main__":
    main()
