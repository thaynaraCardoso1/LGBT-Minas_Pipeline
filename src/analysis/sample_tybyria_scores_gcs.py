import csv
import io
from collections import defaultdict

import pandas as pd
from google.cloud import storage

BUCKET = "lgbtminas-dados"
PREFIX = "rede social/analysis/"  # onde estão os tybyria

TEXT_COL_CANDIDATES = ["body", "text", "comment", "content"]
SCORE_COL = "tybyria_score"

MAX_EXAMPLES_PER_BUCKET = 1


def get_text_column(df):
    for c in TEXT_COL_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError("Nenhuma coluna de texto encontrada")


def bucket_score(score):
    # arredonda para 0.0, 0.1, ..., 1.0
    return round(score * 10) / 10


def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    blobs = list(client.list_blobs(BUCKET, prefix=PREFIX))

    print(f"📦 Arquivos encontrados: {len(blobs)}")

    exemplos = defaultdict(list)

    for blob in blobs:
        if not blob.name.endswith(".csv"):
            continue

        print(f"📄 Lendo: {blob.name}")

        data = blob.download_as_bytes()

        try:
            text = data.decode("utf-8")
        except:
            text = data.decode("latin1")

        try:
            df = pd.read_csv(io.StringIO(text))
        except Exception as e:
            print(f"⚠️ erro lendo {blob.name}: {e}")
            continue

        if SCORE_COL not in df.columns:
            continue

        try:
            text_col = get_text_column(df)
        except:
            continue

        df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce")
        df = df.dropna(subset=[SCORE_COL])

        for _, row in df.iterrows():
            score = row[SCORE_COL]

            if score < 0 or score > 1:
                continue

            bucket = bucket_score(score)

            if len(exemplos[bucket]) >= MAX_EXAMPLES_PER_BUCKET:
                continue

            exemplos[bucket].append({
                "score_real": score,
                "texto": str(row[text_col])[:300]
            })

            # parar cedo se já tem todos
            if len(exemplos) >= 11 and all(len(v) >= 1 for v in exemplos.values()):
                break

        if len(exemplos) >= 11 and all(len(v) >= 1 for v in exemplos.values()):
            break

    print("\n================ RESULTADO ================\n")

    for b in sorted(exemplos.keys()):
        print(f"🔹 SCORE ~ {b}")
        for ex in exemplos[b]:
            print(f"  real: {ex['score_real']}")
            print(f"  texto: {ex['texto']}")
            print("-" * 50)


if __name__ == "__main__":
    main()
