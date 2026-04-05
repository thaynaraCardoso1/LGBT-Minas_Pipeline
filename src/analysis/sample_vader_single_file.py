import io
import pandas as pd
from google.cloud import storage

BUCKET = "lgbtminas-dados"
BLOB = "rede social/analysis/vader/RC_2025-12_BR_vader.csv"

TEXT_COL_CANDIDATES = [
    "text_original",
    "text_clean",
    "body",
    "text",
    "comment",
    "content",
]

SCORE_COL_CANDIDATES = [
    "vader_compound",
    "compound",
    "vader_score",
    "score",
]


def get_text_column(df):
    for c in TEXT_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError(f"Nenhuma coluna de texto encontrada. Colunas disponíveis: {list(df.columns)}")


def get_score_column(df):
    for c in SCORE_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError(f"Nenhuma coluna de score encontrada. Colunas disponíveis: {list(df.columns)}")


def bucket_score(score):
    return round(score * 5) / 5  # buckets de 0.2


def main():
    print("📥 Baixando arquivo...")
    print(f"gs://{BUCKET}/{BLOB}")

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(BLOB)

    data = blob.download_as_bytes()

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("latin1")

    df = pd.read_csv(io.StringIO(text))

    print("📊 Total linhas:", len(df))
    print("🧱 Colunas:", list(df.columns))

    text_col = get_text_column(df)
    score_col = get_score_column(df)

    print("📝 Coluna de texto usada:", text_col)
    print("🎯 Coluna de score usada:", score_col)

    df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
    df = df.dropna(subset=[score_col])

    exemplos = {}

    for _, row in df.iterrows():
        score = row[score_col]

        if score < -1 or score > 1:
            continue

        b = bucket_score(score)

        if b not in exemplos:
            exemplos[b] = {
                "score_real": score,
                "texto": str(row[text_col])[:500].replace("\n", " ")
            }

        if len(exemplos) == 11:
            break

    print("\n================ RESULTADO ================\n")

    alvos = [round(x, 1) for x in [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0]]

    for alvo in alvos:
        if alvo in exemplos:
            print(f"🔹 SCORE ~ {alvo}")
            print(f"real: {exemplos[alvo]['score_real']}")
            print(f"texto: {exemplos[alvo]['texto']}")
        else:
            print(f"🔹 SCORE ~ {alvo}")
            print("real: NÃO ENCONTRADO")
            print("texto: ---")
        print("-" * 80)


if __name__ == "__main__":
    main()
