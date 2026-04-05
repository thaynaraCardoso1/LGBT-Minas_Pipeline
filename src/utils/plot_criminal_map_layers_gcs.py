import io
import os
import sys
import csv
import math
import logging
from datetime import datetime

import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
from branca.element import MacroElement, Template
from google.cloud import storage

# =========================
# CONFIG
# =========================
BUCKET_NAME = "lgbtminas-dados"
INPUT_BLOB = "criminal/processed/2026-02-23 - DIS - Registros - Eventos de LGBTQIAfobia - Jan 2016 a Jan 2025_geocoded.csv"
OUTPUT_BLOB = "criminal/maps/mapa_criminal_cluster_heatmap_camadas.html"
LOG_DIR = "logs"

LAT_COL = "latitude"
LON_COL = "longitude"
YEAR_COL = "Ano"
NATURE_COL = "Natureza Principal"

# limites para não explodir o LayerControl
MAX_NATURES = 12


# =========================
# LOG
# =========================
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"plot_criminal_map_layers_{ts}.log")

    logger = logging.getLogger("plot_criminal_map_layers")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

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
# CSV / LIMPEZA
# =========================
def read_csv_from_text(text, logger):
    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    logger.info("Delimitador detectado: %r", delimiter)

    df = pd.read_csv(io.StringIO(text), delimiter=delimiter, encoding="latin1")

    df.columns = (
        df.columns
        .str.replace("\xad", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed:")]
    if unnamed_cols:
        logger.info("Removendo colunas extras: %s", unnamed_cols)
        df = df.drop(columns=unnamed_cols)

    return df


def normalizar_coord(valor):
    if pd.isna(valor):
        return None

    s = str(valor).strip()
    if not s:
        return None

    s = s.replace(" ", "")

    try:
        return float(s)
    except Exception:
        pass

    negativo = s.startswith("-")
    s_limpo = s.replace("-", "").replace(".", "").replace(",", "")

    if not s_limpo.isdigit() or len(s_limpo) < 3:
        return None

    s_corrigido = s_limpo[:2] + "." + s_limpo[2:]
    v = float(s_corrigido)

    if negativo:
        v = -v

    return v


def preparar_dataframe(df, logger):
    total_inicial = len(df)
    logger.info("Total inicial de registros: %s", total_inicial)

    if LAT_COL not in df.columns or LON_COL not in df.columns:
        raise ValueError(f"Colunas {LAT_COL!r} e/ou {LON_COL!r} não encontradas.")

    df = df.copy()

    df[LAT_COL] = df[LAT_COL].apply(normalizar_coord)
    df[LON_COL] = df[LON_COL].apply(normalizar_coord)

    df = df.dropna(subset=[LAT_COL, LON_COL])

    df = df[
        df[LAT_COL].between(-25, -13) &
        df[LON_COL].between(-51, -39)
    ]

    if YEAR_COL in df.columns:
        df[YEAR_COL] = pd.to_numeric(df[YEAR_COL], errors="coerce").astype("Int64")

    if NATURE_COL in df.columns:
        df[NATURE_COL] = df[NATURE_COL].astype(str).str.strip()
        df.loc[df[NATURE_COL].isin(["", "nan", "None"]), NATURE_COL] = "Não informado"

    logger.info("Total após limpeza de coordenadas: %s", len(df))
    logger.info("Total descartado: %s", total_inicial - len(df))

    return df


# =========================
# VISUAL
# =========================
def colorir_natureza(nome):
    nome = (nome or "").upper()

    if "AMEACA" in nome:
        return "red"
    if "LESAO" in nome:
        return "orange"
    if "HOMICID" in nome:
        return "darkred"
    if "INJURIA" in nome:
        return "purple"
    if "VIA DE FATO" in nome:
        return "blue"
    return "cadetblue"


def build_popup(row):
    campos_prioritarios = [
        "ID Ocorrência",
        "Data/Hora Ocorrência",
        YEAR_COL,
        NATURE_COL,
        "Bairro",
        "Município (Fato)",
        "UF - Sigla",
    ]

    partes = []
    for col in campos_prioritarios:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip() != "":
            partes.append(f"<b>{col}</b>: {row[col]}")

    return "<br>".join(partes) if partes else "Sem detalhes"


def adicionar_legenda(mapa):
    legenda_html = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed;
        bottom: 40px;
        left: 40px;
        z-index: 9999;
        background-color: white;
        border: 2px solid #999;
        border-radius: 8px;
        padding: 10px 12px;
        font-size: 13px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.25);
    ">
      <div style="font-weight: bold; margin-bottom: 6px;">Legenda</div>
      <div><span style="color:red;">●</span> Ameaça</div>
      <div><span style="color:orange;">●</span> Lesão</div>
      <div><span style="color:darkred;">●</span> Homicídio</div>
      <div><span style="color:purple;">●</span> Injúria</div>
      <div><span style="color:blue;">●</span> Via de fato</div>
      <div><span style="color:cadetblue;">●</span> Outras naturezas</div>
      <hr style="margin:6px 0;">
      <div>Heatmap = concentração</div>
      <div>Cluster = ocorrências agrupadas</div>
    </div>
    {% endmacro %}
    """
    macro = MacroElement()
    macro._template = Template(legenda_html)
    mapa.get_root().add_child(macro)


def adicionar_marcadores(feature_group, df_subset):
    cluster = MarkerCluster().add_to(feature_group)

    for _, row in df_subset.iterrows():
        popup_html = build_popup(row)
        cor = colorir_natureza(row.get(NATURE_COL, ""))

        folium.CircleMarker(
            location=[row[LAT_COL], row[LON_COL]],
            radius=4,
            color=cor,
            fill=True,
            fill_color=cor,
            fill_opacity=0.65,
            weight=1,
            popup=folium.Popup(popup_html, max_width=320),
        ).add_to(cluster)


def adicionar_heatmap(feature_group, df_subset):
    pontos = df_subset[[LAT_COL, LON_COL]].dropna().values.tolist()
    if pontos:
        HeatMap(
            pontos,
            radius=18,
            blur=14,
            min_opacity=0.25,
            max_zoom=12,
        ).add_to(feature_group)


def criar_mapa(df, logger):
    centro_lat = df[LAT_COL].mean()
    centro_lon = df[LON_COL].mean()

    mapa = folium.Map(
        location=[centro_lat, centro_lon],
        zoom_start=6,
        tiles="CartoDB positron"
    )

    # -------------------------
    # CAMADAS GERAIS
    # -------------------------
    fg_cluster_geral = folium.FeatureGroup(name="Cluster - Geral", show=True)
    adicionar_marcadores(fg_cluster_geral, df)
    fg_cluster_geral.add_to(mapa)

    fg_heat_geral = folium.FeatureGroup(name="Heatmap - Geral", show=False)
    adicionar_heatmap(fg_heat_geral, df)
    fg_heat_geral.add_to(mapa)

    # -------------------------
    # CAMADAS POR ANO
    # -------------------------
    if YEAR_COL in df.columns:
        anos = sorted([int(a) for a in df[YEAR_COL].dropna().unique()])
        logger.info("Anos encontrados: %s", anos)

        for ano in anos:
            df_ano = df[df[YEAR_COL] == ano]

            fg_cluster_ano = folium.FeatureGroup(name=f"Cluster - Ano {ano}", show=False)
            adicionar_marcadores(fg_cluster_ano, df_ano)
            fg_cluster_ano.add_to(mapa)

            fg_heat_ano = folium.FeatureGroup(name=f"Heatmap - Ano {ano}", show=False)
            adicionar_heatmap(fg_heat_ano, df_ano)
            fg_heat_ano.add_to(mapa)

    # -------------------------
    # CAMADAS POR NATUREZA
    # -------------------------
    if NATURE_COL in df.columns:
        naturezas = (
            df[NATURE_COL]
            .fillna("Não informado")
            .value_counts()
            .head(MAX_NATURES)
            .index
            .tolist()
        )

        logger.info("Naturezas selecionadas para camada: %s", naturezas)

        for natureza in naturezas:
            df_nat = df[df[NATURE_COL] == natureza]

            nome_limpo = natureza[:60]

            fg_cluster_nat = folium.FeatureGroup(name=f"Cluster - Natureza: {nome_limpo}", show=False)
            adicionar_marcadores(fg_cluster_nat, df_nat)
            fg_cluster_nat.add_to(mapa)

            fg_heat_nat = folium.FeatureGroup(name=f"Heatmap - Natureza: {nome_limpo}", show=False)
            adicionar_heatmap(fg_heat_nat, df_nat)
            fg_heat_nat.add_to(mapa)

    # -------------------------
    # LEGENDA E CONTROLE
    # -------------------------
    adicionar_legenda(mapa)
    folium.LayerControl(collapsed=False).add_to(mapa)

    return mapa


# =========================
# MAIN
# =========================
def main():
    logger = setup_logger()
    client = storage.Client()

    text = download_blob_as_text(client, BUCKET_NAME, INPUT_BLOB, logger)
    df = read_csv_from_text(text, logger)
    df = preparar_dataframe(df, logger)

    if df.empty:
        raise ValueError("Nenhum registro válido restou após a limpeza.")

    logger.info("Distribuição por ano:")
    if YEAR_COL in df.columns:
        logger.info("\n%s", df[YEAR_COL].value_counts(dropna=False).sort_index().to_string())

    logger.info("Top naturezas:")
    if NATURE_COL in df.columns:
        logger.info("\n%s", df[NATURE_COL].value_counts(dropna=False).head(20).to_string())

    mapa = criar_mapa(df, logger)
    html = mapa.get_root().render()

    upload_text_to_gcs(
        client=client,
        bucket_name=BUCKET_NAME,
        blob_name=OUTPUT_BLOB,
        text=html,
        logger=logger,
        content_type="text/html"
    )

    logger.info("Mapa final gerado com sucesso.")
    logger.info("Arquivo final: gs://%s/%s", BUCKET_NAME, OUTPUT_BLOB)
    logger.info("Total de registros plotados: %s", len(df))


if __name__ == "__main__":
    main()
