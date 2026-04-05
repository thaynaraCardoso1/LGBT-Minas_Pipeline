import io
import traceback
import pandas as pd
from google.cloud import storage

BUCKET = "lgbtminas-dados"

RAW_PREFIX = "criminal/raw/"
GEOCODED_UNIQUE_PATH = "criminal/processed/bairros_municipios_geocoded.csv"
OUTPUT_PATH = "criminal/processed/registros_geocoded.csv"

LOCAL_TMP = "/tmp/registros_geocoded.csv"

COL_BAIRRO_RAW = "Bairro"
COL_MUNICIPIO_CODIGO_RAW = "Município (Código)"
COL_MUNICIPIO_NOME_RAW = "Município (Fato)"

COL_BAIRRO_GEO = "bairro"
COL_MUNICIPIO_CODIGO_GEO = "municipio_codigo"
COL_MUNICIPIO_NOME_GEO = "municipio_nome"


def load_csv_blob(blob):
    raw = blob.download_as_bytes()

    last_error = None
    for enc in ["utf-8", "latin1", "cp1252", "iso-8859-1"]:
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=None, engine="python")
            print(f"✅ CSV lido com encoding: {enc} | {blob.name}")
            return df
        except Exception as e:
            last_error = e
            print(f"⚠️ Falhou com encoding={enc} em {blob.name}: {e}")

    raise Exception(f"❌ Não foi possível ler o CSV {blob.name}. Último erro: {last_error}")


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_first_csv_blob(client, bucket_name, prefix):
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    csvs = [b for b in blobs if b.name.endswith(".csv")]

    if not csvs:
        raise Exception(f"❌ Nenhum CSV encontrado em {prefix}")

    if len(csvs) > 1:
        print(f"⚠️ Mais de um CSV encontrado em {prefix}. Usando o primeiro da lista:")
        for b in csvs:
            print(f" - {b.name}")

    return csvs[0]


def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    print("🔎 Localizando CSV raw...")
    raw_blob = find_first_csv_blob(client, BUCKET, RAW_PREFIX)
    print(f"📄 Raw: {raw_blob.name}")

    print("🔎 Lendo CSV raw...")
    df_raw = load_csv_blob(raw_blob)
    print(f"📊 Linhas raw: {len(df_raw)}")

    print("🔎 Lendo CSV geocodificado único...")
    geo_blob = bucket.blob(GEOCODED_UNIQUE_PATH)
    if not geo_blob.exists(client):
        raise Exception(f"❌ Arquivo não encontrado: gs://{BUCKET}/{GEOCODED_UNIQUE_PATH}")

    df_geo = load_csv_blob(geo_blob)
    print(f"📊 Linhas geocodificadas únicas: {len(df_geo)}")

    required_raw = [COL_BAIRRO_RAW, COL_MUNICIPIO_CODIGO_RAW, COL_MUNICIPIO_NOME_RAW]
    missing_raw = [c for c in required_raw if c not in df_raw.columns]
    if missing_raw:
        raise Exception(f"❌ Colunas ausentes no raw: {missing_raw}")

    required_geo = [
        COL_BAIRRO_GEO,
        COL_MUNICIPIO_CODIGO_GEO,
        COL_MUNICIPIO_NOME_GEO,
        "latitude",
        "longitude",
    ]
    missing_geo = [c for c in required_geo if c not in df_geo.columns]
    if missing_geo:
        raise Exception(f"❌ Colunas ausentes no geocodificado: {missing_geo}")

    print("🧹 Normalizando chaves...")
    df_raw["_bairro_key"] = df_raw[COL_BAIRRO_RAW].apply(normalize_text)
    df_raw["_municipio_codigo_key"] = df_raw[COL_MUNICIPIO_CODIGO_RAW].apply(normalize_text)
    df_raw["_municipio_nome_key"] = df_raw[COL_MUNICIPIO_NOME_RAW].apply(normalize_text)

    df_geo["_bairro_key"] = df_geo[COL_BAIRRO_GEO].apply(normalize_text)
    df_geo["_municipio_codigo_key"] = df_geo[COL_MUNICIPIO_CODIGO_GEO].apply(normalize_text)
    df_geo["_municipio_nome_key"] = df_geo[COL_MUNICIPIO_NOME_GEO].apply(normalize_text)

    df_geo = df_geo[
        [
            "_bairro_key",
            "_municipio_codigo_key",
            "_municipio_nome_key",
            "latitude",
            "longitude",
        ]
    ].drop_duplicates()

    print("🔗 Fazendo merge...")
    df_final = df_raw.merge(
        df_geo,
        how="left",
        on=["_bairro_key", "_municipio_codigo_key", "_municipio_nome_key"]
    )

    matched = df_final["latitude"].notna().sum()
    unmatched = len(df_final) - matched

    print(f"✅ Linhas com latitude/longitude: {matched}")
    print(f"❌ Linhas sem latitude/longitude: {unmatched}")

    df_final = df_final.drop(
        columns=["_bairro_key", "_municipio_codigo_key", "_municipio_nome_key"],
        errors="ignore"
    )

    print(f"💾 Salvando localmente em {LOCAL_TMP} ...")
    df_final.to_csv(LOCAL_TMP, index=False)

    print("☁️ Enviando para GCS...")
    out_blob = bucket.blob(OUTPUT_PATH)
    out_blob.upload_from_filename(LOCAL_TMP, content_type="text/csv")

    print("🔎 Confirmando upload...")
    exists = out_blob.exists(client)
    print(f"Arquivo existe no bucket? {exists}")

    if not exists:
        raise Exception("❌ Upload não foi persistido no bucket")

    print("✅ Finalizado")
    print(f"Arquivo salvo em: gs://{BUCKET}/{OUTPUT_PATH}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("💥 ERRO NO SCRIPT")
        print(str(e))
        traceback.print_exc()
        raise
