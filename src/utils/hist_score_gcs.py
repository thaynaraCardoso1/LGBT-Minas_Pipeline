import os
import csv
import io
from typing import Dict, List
from statistics import mean
from google.cloud import storage
import matplotlib.pyplot as plt

# =======================
# CONSTANTES (AJUSTE AQUI)
# =======================
BUCKET_NAME = "lgbtminas-dados"

# Tybyria está misturado em /analysis, então filtramos pelo sufixo do arquivo
PREFIX = "rede social/analysis/vader"
FILE_SUFFIX = "_vader.csv"

# Coluna que contém o score do Tybyria
SCORE_COL = "vader_compound"

# Saídas (local na VM)
OUTPUT_DIR = "saida/hist"
OUT_PNG = "hist_vader_score.png"

# Range do Tybyria score: [0, 1]
HIST_MIN = 0.0
HIST_MAX = 1.0
BINS = 50
# =======================


def is_blank_row(row: Dict[str, str]) -> bool:
    """Linha em branco quando TODAS as colunas estão vazias/espacos."""
    for v in row.values():
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=PREFIX))

    scores: List[float] = []
    used_files = 0
    total_rows = 0
    blank_rows = 0
    bad_score = 0
    out_of_range = 0

    for blob in blobs:
        name = blob.name
        lname = name.lower()

        # ✅ só arquivos do tybyria
        if not lname.endswith(FILE_SUFFIX):
            continue

        used_files += 1
        print(f"📄 Lendo: gs://{BUCKET_NAME}/{name}")

        with blob.open("rb") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))

            if not reader.fieldnames or SCORE_COL not in reader.fieldnames:
                print(f"⚠️ Pulando {name}: não tem coluna '{SCORE_COL}'")
                continue

            for row in reader:
                total_rows += 1

                if is_blank_row(row):
                    blank_rows += 1
                    continue

                raw = row.get(SCORE_COL, "")
                if raw is None or str(raw).strip() == "":
                    bad_score += 1
                    continue

                try:
                    val = float(raw)
                except:
                    bad_score += 1
                    continue

                if not (HIST_MIN <= val <= HIST_MAX):
                    out_of_range += 1
                    continue

                scores.append(val)

    if used_files == 0:
        raise RuntimeError(f"❌ Não achei nenhum arquivo *{FILE_SUFFIX} em gs://{BUCKET_NAME}/{PREFIX}")

    if not scores:
        raise RuntimeError("❌ Nenhum score válido foi coletado. Verifique a coluna e os arquivos.")

    print("\n✅ OK!")
    print(f"Prefix: gs://{BUCKET_NAME}/{PREFIX}")
    print(f"Arquivos Vader lidos: {used_files}")
    print(f"Linhas lidas: {total_rows}")
    print(f"Linhas em branco removidas: {blank_rows}")
    print(f"Linhas sem score válido: {bad_score}")
    print(f"Scores fora do range [{HIST_MIN}, {HIST_MAX}]: {out_of_range}")
    print(f"Scores no histograma: {len(scores)}")
    print(f"Min/Max: {min(scores):.4f} / {max(scores):.4f}")
    print(f"Média: {mean(scores):.4f}")

    plt.figure()
    plt.hist(scores, bins=BINS, range=(HIST_MIN, HIST_MAX))
    plt.xlabel(SCORE_COL)
    plt.ylabel("Frequência")
    plt.title("Histograma — tybyria_score (0 a 1)")
    out_path = os.path.join(OUTPUT_DIR, OUT_PNG)
    plt.savefig(out_path, dpi=150)
    print(f"📊 PNG: {out_path}")


if __name__ == "__main__":
    main()
