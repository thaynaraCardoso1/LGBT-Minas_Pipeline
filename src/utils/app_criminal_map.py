import io
import csv
from typing import Optional

import pandas as pd
import streamlit as st
import folium
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium
from google.cloud import storage
from branca.element import MacroElement, Template

# =========================
# CONFIG
# =========================
BUCKET_NAME = "lgbtminas-dados"
INPUT_BLOB = "criminal/processed/2026-02-23 - DIS - Registros - Eventos de LGBTQIAfobia - Jan 2016 a Jan 2025_geocoded.csv"

LAT_COL = "latitude"
LON_COL = "longitude"
YEAR_COL = "Ano"
NATURE_COL = "Natureza Principal"
CITY_COL = "Município (Fato)"
NEIGHBOR_COL = "Bairro"

# =========================
# PAGE
# =========================
st.set_page_config(
    page_title="Mapa interativo de LGBTQIAfobia em MG",
    layout="wide"
)

st.title("Mapa interativo de registros de LGBTQIAfobia em Minas Gerais")
st.caption("Filtros combináveis por ano, tipo de violência e tipo de mapa")

# =========================
# GCS
# =========================
@st.cache_data(show_spinner=False)
def download_blob_as_text(bucket_name: str, blob_name: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin1")

# =========================
# HELPERS
# =========================
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.replace("\xad", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )

    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed:")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)

    return df


def normalizar_coord(valor) -> Optional[float]:
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

    # corrige casos tipo -218.474.746 -> -21.8474746
    negativo = s.startswith("-")
    s_limpo = s.replace("-", "").replace(".", "").replace(",", "")

    if not s_limpo.isdigit() or len(s_limpo) < 3:
        return None

    s_corrigido = s_limpo[:2] + "." + s_limpo[2:]
    v = float(s_corrigido)
    return -v if negativo else v


@st.cache_data(show_spinner=True)
def carregar_dados() -> pd.DataFrame:
    text = download_blob_as_text(BUCKET_NAME, INPUT_BLOB)

    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    df = pd.read_csv(io.StringIO(text), delimiter=delimiter, encoding="latin1")
    df = normalizar_colunas(df)

    df[LAT_COL] = df[LAT_COL].apply(normalizar_coord)
    df[LON_COL] = df[LON_COL].apply(normalizar_coord)

    df = df.dropna(subset=[LAT_COL, LON_COL])

    # filtro geográfico aproximado para MG
    df = df[
        df[LAT_COL].between(-25, -13) &
        df[LON_COL].between(-51, -39)
    ].copy()

    if YEAR_COL in df.columns:
        df[YEAR_COL] = pd.to_numeric(df[YEAR_COL], errors="coerce").astype("Int64")

    if NATURE_COL in df.columns:
        df[NATURE_COL] = df[NATURE_COL].astype(str).str.strip()
        df.loc[df[NATURE_COL].isin(["", "nan", "None"]), NATURE_COL] = "Não informado"

    for col in [CITY_COL, NEIGHBOR_COL]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def colorir_natureza(nome: str) -> str:
    nome = (nome or "").upper()

    if "AMEA" in nome:
        return "red"
    if "LESA" in nome:
        return "orange"
    if "HOMIC" in nome:
        return "darkred"
    if "INJUR" in nome:
        return "purple"
    if "VIA DE FATO" in nome:
        return "blue"

    return "cadetblue"


def popup_html(row: pd.Series) -> str:
    campos = [
        "ID Ocorrência",
        "Data/Hora Ocorrência",
        YEAR_COL,
        NATURE_COL,
        CITY_COL,
        NEIGHBOR_COL,
    ]

    partes = []
    for c in campos:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
            partes.append(f"<b>{c}</b>: {row[c]}")

    return "<br>".join(partes) if partes else "Sem detalhes"


def adicionar_legenda(mapa):
    legenda_html = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed;
        bottom: 35px;
        left: 35px;
        z-index: 9999;
        background: white;
        border: 2px solid #999;
        border-radius: 8px;
        padding: 10px 12px;
        font-size: 13px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    ">
      <div style="font-weight:bold; margin-bottom:6px;">Legenda</div>
      <div><span style="color:red;">●</span> Ameaça</div>
      <div><span style="color:orange;">●</span> Lesão</div>
      <div><span style="color:darkred;">●</span> Homicídio</div>
      <div><span style="color:purple;">●</span> Injúria</div>
      <div><span style="color:blue;">●</span> Via de fato</div>
      <div><span style="color:cadetblue;">●</span> Outras</div>
      <hr style="margin:6px 0;">
      <div><b>Heatmap:</b> concentração espacial</div>
      <div><b>Cluster:</b> agrupamento visual por proximidade</div>
    </div>
    {% endmacro %}
    """
    macro = MacroElement()
    macro._template = Template(legenda_html)
    mapa.get_root().add_child(macro)


def adicionar_heatmap(mapa, df_filtrado: pd.DataFrame):
    pontos = df_filtrado[[LAT_COL, LON_COL]].dropna().values.tolist()

    HeatMap(
        pontos,
        radius=26,
        blur=18,
        min_opacity=0.35,
        max_zoom=15,
        gradient={
            0.05: "#e6e1ff",
            0.18: "#c9c2ff",
            0.32: "#9b8cff",
            0.48: "#63d7ff",
            0.62: "#49f0c1",
            0.78: "#7dff4d",
            0.90: "#ffe84a",
            1.00: "#ff3b1f",
        },
    ).add_to(mapa)


def adicionar_cluster(mapa, df_filtrado: pd.DataFrame):
    cluster = MarkerCluster(
        options={
            "showCoverageOnHover": False,
            "spiderfyOnMaxZoom": True,
            "disableClusteringAtZoom": 14,
            "maxClusterRadius": 40,
        }
    ).add_to(mapa)

    for _, row in df_filtrado.iterrows():
        cor = colorir_natureza(row.get(NATURE_COL, ""))

        folium.CircleMarker(
            location=[row[LAT_COL], row[LON_COL]],
            radius=4,
            color=cor,
            fill=True,
            fill_color=cor,
            fill_opacity=0.75,
            weight=1,
            popup=folium.Popup(popup_html(row), max_width=320),
        ).add_to(cluster)


def criar_mapa(df_filtrado: pd.DataFrame, tipo_mapa: str):
    center_lat = float(df_filtrado[LAT_COL].mean())
    center_lon = float(df_filtrado[LON_COL].mean())

    mapa = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles="CartoDB positron"
    )

    if tipo_mapa == "Cluster":
        adicionar_cluster(mapa, df_filtrado)

    elif tipo_mapa == "Heatmap":
        adicionar_heatmap(mapa, df_filtrado)

    else:  # Ambos
        heat_fg = folium.FeatureGroup(name="Heatmap", show=True)
        adicionar_heatmap(heat_fg, df_filtrado)
        heat_fg.add_to(mapa)

        cluster_fg = folium.FeatureGroup(name="Cluster", show=True)
        adicionar_cluster(cluster_fg, df_filtrado)
        cluster_fg.add_to(mapa)

        folium.LayerControl(collapsed=False).add_to(mapa)

    adicionar_legenda(mapa)
    return mapa


# =========================
# LOAD
# =========================
df = carregar_dados()

if df.empty:
    st.error("Nenhum registro válido foi encontrado.")
    st.stop()

# =========================
# SIDEBAR FILTERS
# =========================
st.sidebar.header("Filtros")

anos_validos = sorted([int(x) for x in df[YEAR_COL].dropna().unique()])
ano_min, ano_max = min(anos_validos), max(anos_validos)

intervalo_anos = st.sidebar.slider(
    "Intervalo de anos",
    min_value=ano_min,
    max_value=ano_max,
    value=(ano_min, ano_max),
)

naturezas = sorted(df[NATURE_COL].dropna().astype(str).unique().tolist())
naturezas_sel = st.sidebar.multiselect(
    "Tipo de violência",
    options=naturezas,
    default=naturezas
)

tipo_mapa = st.sidebar.selectbox(
    "Tipo de mapa",
    options=["Cluster", "Heatmap", "Ambos"],
    index=0
)

mostrar_tabela = st.sidebar.checkbox("Mostrar tabela filtrada", value=False)

# =========================
# FILTER DATA
# =========================
df_filtrado = df[df[YEAR_COL].between(intervalo_anos[0], intervalo_anos[1])].copy()

if naturezas_sel:
    df_filtrado = df_filtrado[df_filtrado[NATURE_COL].isin(naturezas_sel)].copy()
else:
    df_filtrado = df_filtrado.iloc[0:0].copy()

# =========================
# KPI
# =========================
col1, col2, col3 = st.columns(3)
col1.metric("Registros filtrados", f"{len(df_filtrado):,}".replace(",", "."))
col2.metric("Anos selecionados", f"{intervalo_anos[0]}–{intervalo_anos[1]}")
col3.metric("Tipos de violência", len(naturezas_sel))

if df_filtrado.empty:
    st.warning("Nenhum registro encontrado para os filtros selecionados.")
    st.stop()

# =========================
# MAPA
# =========================
mapa = criar_mapa(df_filtrado, tipo_mapa)
st_folium(mapa, use_container_width=True, height=700)

# =========================
# RESUMOS
# =========================
with st.expander("Resumo por ano"):
    resumo_ano = (
        df_filtrado.groupby(YEAR_COL, dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values(YEAR_COL)
    )
    st.dataframe(resumo_ano, use_container_width=True)

with st.expander("Resumo por tipo de violência"):
    resumo_nat = (
        df_filtrado.groupby(NATURE_COL, dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values("quantidade", ascending=False)
    )
    st.dataframe(resumo_nat, use_container_width=True)

if mostrar_tabela:
    st.subheader("Tabela filtrada")
    st.dataframe(df_filtrado, use_container_width=True)
