import io
import pandas as pd
from google.cloud import storage

BUCKET = "lgbtminas-dados"
BLOB = "rede social/analysis/tybyria/RC_2025-12_BR_tybyria.csv"

TEXT_COL_CANDIDATES = [
    "text_original",
    "text_clean",
    "body",
    "text",
    "comment",
    "content",
]
SCORE_COL = "tybyria_score"


def get_text_column(df):
    for c in TEXT_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError(f"Nenhuma coluna de texto encontrada. Colunas disponíveis: {list(df.columns)}")


def bucket_score(score):
    return round(score * 10) / 10


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

    df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce")
    df = df.dropna(subset=[SCORE_COL])

    text_col = get_text_column(df)
    print("📝 Coluna de texto usada:", text_col)

    exemplos = {}

    for _, row in df.iterrows():
        score = row[SCORE_COL]

        if score < 0 or score > 1:
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

    for alvo in [round(x / 10, 1) for x in range(11)]:
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
