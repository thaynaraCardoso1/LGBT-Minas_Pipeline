import csv
import io
import os
import re
from collections import Counter
from typing import Dict, List, Optional, Tuple

from google.cloud import storage

# =======================
# CONSTANTES
# =======================
BUCKET_NAME = "lgbtminas-dados"
PREFIX = "rede social/processed/"
OUTPUT_DIR = "saida/counts"
OUTPUT_WIDE_CSV = "count_by_file_subreddit_wide.csv"
OUTPUT_LONG_CSV = "count_by_file_subreddit_long.csv"
# =======================

# Possíveis nomes de coluna para subreddit
SUBREDDIT_COL_CANDIDATES = [
    "subreddit",
    "subreddit_name",
    "subredditname",
    "sub",
    "community",
    "community_name",
]

# Regex para aceitar somente:
# RC_2023-01_BR.csv, RC_2024-12_BR.csv, etc.
VALID_FILENAME_RE = re.compile(r"^RC_\d{4}-\d{2}_BR\.csv$", re.IGNORECASE)


def is_blank_row(row: Dict[str, str]) -> bool:
    for v in row.values():
        if v is None:
            continue
        if str(v).strip():
            return False
    return True


def detect_subreddit_col(fieldnames: List[str]) -> Optional[str]:
    if not fieldnames:
        return None

    normalized = {(f or "").strip().lower(): f for f in fieldnames}

    for cand in SUBREDDIT_COL_CANDIDATES:
        if cand in normalized:
            return normalized[cand]

    for k, original in normalized.items():
        if "subreddit" in k:
            return original

    return None


def iter_csv_blobs(client: storage.Client, bucket: storage.Bucket, prefix: str):
    for blob in client.list_blobs(bucket, prefix=prefix):
        name = blob.name

        if name.endswith("/"):
            continue

        base = os.path.basename(name)

        # Ignora arquivos antigos
        if base.lower().endswith("_old.csv"):
            continue

        # Aceita somente RC_YYYY-MM_BR.csv
        if not VALID_FILENAME_RE.match(base):
            continue

        yield blob


def count_subreddits_in_csv_blob(blob: storage.Blob) -> Tuple[Counter, int, Optional[str]]:
    """
    Retorna:
      - Counter(subreddit -> count)
      - total de linhas não brancas
      - nome da coluna subreddit detectada
    """
    counts = Counter()
    non_blank_records = 0

    with blob.open("rb") as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
        reader = csv.DictReader(text)
        sub_col = detect_subreddit_col(reader.fieldnames or [])

        for row in reader:
            if not row or is_blank_row(row):
                continue

            non_blank_records += 1

            if sub_col:
                sub = (row.get(sub_col) or "").strip()
                if sub:
                    counts[sub] += 1
                else:
                    counts["(sem_subreddit)"] += 1
            else:
                counts["(coluna_subreddit_nao_encontrada)"] += 1

    return counts, non_blank_records, sub_col


def print_markdown_table(rows: List[Dict[str, int]], columns: List[str]):
    header = ["arquivo"] + columns
    print("| " + " | ".join(header) + " |")
    print("| " + " | ".join(["---"] * len(header)) + " |")

    for r in rows:
        line = [r.get("arquivo", "")]
        for c in columns:
            line.append(str(r.get(c, 0)))
        print("| " + " | ".join(line) + " |")


def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    per_file_counts: Dict[str, Counter] = {}
    per_file_non_blank: Dict[str, int] = {}
    detected_cols: Dict[str, Optional[str]] = {}

    blobs = list(iter_csv_blobs(client, bucket, PREFIX))

    if not blobs:
        print(f"Nenhum CSV RC_*_BR.csv encontrado em gs://{BUCKET_NAME}/{PREFIX}")
        return 1

    print(f"Encontrados {len(blobs)} CSVs válidos em gs://{BUCKET_NAME}/{PREFIX}")

    for blob in blobs:
        counts, non_blank, sub_col = count_subreddits_in_csv_blob(blob)

        per_file_counts[blob.name] = counts
        per_file_non_blank[blob.name] = non_blank
        detected_cols[blob.name] = sub_col

        print(f"- {blob.name}: {non_blank} linhas não brancas | subreddit_col={sub_col}")

    total_by_sub = Counter()
    for c in per_file_counts.values():
        total_by_sub.update(c)

    subreddit_cols = [s for s, _ in total_by_sub.most_common()]

    wide_rows: List[Dict[str, int]] = []
    for fname in sorted(per_file_counts.keys()):
        row: Dict[str, int] = {
            "arquivo": fname,
            "TOTAL": per_file_non_blank.get(fname, 0),
        }
        counts = per_file_counts[fname]
        for s in subreddit_cols:
            row[s] = counts.get(s, 0)
        wide_rows.append(row)

    print("\n=== TABELA (arquivo x subreddit) ===")
    print_markdown_table(wide_rows, ["TOTAL"] + subreddit_cols)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    wide_path = os.path.join(OUTPUT_DIR, OUTPUT_WIDE_CSV)
    long_path = os.path.join(OUTPUT_DIR, OUTPUT_LONG_CSV)

    wide_fieldnames = ["arquivo", "TOTAL"] + subreddit_cols
    with open(wide_path, "w", encoding="utf-8", newline="") as wf:
        writer = csv.DictWriter(wf, fieldnames=wide_fieldnames)
        writer.writeheader()
        for row in wide_rows:
            writer.writerow(row)

    with open(long_path, "w", encoding="utf-8", newline="") as lf:
        writer = csv.writer(lf)
        writer.writerow(["arquivo", "subreddit", "count"])
        for fname in sorted(per_file_counts.keys()):
            counts = per_file_counts[fname]
            for subreddit, count in counts.items():
                writer.writerow([fname, subreddit, count])

    print("\nArquivos gerados:")
    print(f"- {wide_path}")
    print(f"- {long_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
