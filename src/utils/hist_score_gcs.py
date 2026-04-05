import os
import csv
import io
from typing import Dict, List, Callable, Optional
from statistics import mean

from google.cloud import storage
import matplotlib.pyplot as plt


# =======================
# CONSTANTES
# =======================
BUCKET_NAME = "lgbtminas-dados"

PREFIX_TYBYRIA = "rede social/analysis/tybyria/"
PREFIX_VADER = "rede social/analysis/vader/"

FILE_SUFFIX_TYBYRIA = "_tybyria.csv"
FILE_SUFFIX_VADER = "_vader.csv"

SCORE_COL_TYBYRIA = "tybyria_score"
SCORE_COL_VADER = "vader_compound"

OUTPUT_DIR = "saida/hist"

OUT_PNG_TYBYRIA = "hist_tybyria_score.png"
OUT_PNG_VADER = "hist_vader_compound_normalized.png"
OUT_PNG_COMPARE = "hist_comparativo_tybyria_vader.png"

HIST_MIN = 0.0
HIST_MAX = 1.0
BINS = 50
# =======================


def is_blank_row(row: Dict[str, str]) -> bool:
    for v in row.values():
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True


def normalize_identity(x: float) -> float:
    return x


def normalize_vader_to_01(x: float) -> float:
    return (x + 1.0) / 2.0


def collect_scores(
    client: storage.Client,
    bucket_name: str,
    prefix: str,
    file_suffix: str,
    score_col: str,
    hist_min: float,
    hist_max: float,
    label: str,
    normalizer: Optional[Callable[[float], float]] = None,
    raw_min: Optional[float] = None,
    raw_max: Optional[float] = None,
) -> List[float]:
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))

    scores: List[float] = []
    used_files = 0
    total_rows = 0
    blank_rows = 0
    bad_score = 0
    out_of_range_raw = 0
    out_of_range_final = 0

    for blob in blobs:
        name = blob.name
        lname = name.lower()

        if not lname.endswith(file_suffix):
            continue

        used_files += 1
        print(f"📄 [{label}] Lendo: gs://{bucket_name}/{name}")

        with blob.open("rb") as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            )

            if not reader.fieldnames or score_col not in reader.fieldnames:
                print(f"⚠️ [{label}] Pulando {name}: não tem coluna '{score_col}'")
                continue

            for row in reader:
                total_rows += 1

                if is_blank_row(row):
                    blank_rows += 1
                    continue

                raw = row.get(score_col, "")
                if raw is None or str(raw).strip() == "":
                    bad_score += 1
                    continue

                try:
                    val = float(raw)
                except Exception:
                    bad_score += 1
                    continue

                if raw_min is not None and raw_max is not None:
                    if not (raw_min <= val <= raw_max):
                        out_of_range_raw += 1
                        continue

                if normalizer is not None:
                    val = normalizer(val)

                if not (hist_min <= val <= hist_max):
                    out_of_range_final += 1
                    continue

                scores.append(val)

    if used_files == 0:
        raise RuntimeError(
            f"❌ [{label}] Não achei nenhum arquivo *{file_suffix} em gs://{bucket_name}/{prefix}"
        )

    if not scores:
        raise RuntimeError(
            f"❌ [{label}] Nenhum score válido foi coletado. Verifique a coluna e os arquivos."
        )

    print(f"\n✅ [{label}] OK!")
    print(f"Prefix: gs://{bucket_name}/{prefix}")
    print(f"Arquivos lidos: {used_files}")
    print(f"Linhas lidas: {total_rows}")
    print(f"Linhas em branco removidas: {blank_rows}")
    print(f"Linhas sem score válido: {bad_score}")
    print(f"Scores fora do range bruto: {out_of_range_raw}")
    print(f"Scores fora do range final [{hist_min}, {hist_max}]: {out_of_range_final}")
    print(f"Scores no histograma: {len(scores)}")
    print(f"Min/Max final: {min(scores):.4f} / {max(scores):.4f}")
    print(f"Média final: {mean(scores):.4f}")
    print()

    return scores


def save_histogram(
    scores: List[float],
    out_path: str,
    title: str,
    xlabel: str,
    hist_min: float,
    hist_max: float,
    bins: int,
):
    plt.figure(figsize=(10, 6))
    plt.hist(scores, bins=bins, range=(hist_min, hist_max), edgecolor="black")
    plt.xlabel(xlabel)
    plt.ylabel("Frequência")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"📊 PNG: {out_path}")


def save_comparative_histogram(
    tybyria_scores: List[float],
    vader_scores: List[float],
    out_path: str,
    hist_min: float,
    hist_max: float,
    bins: int,
):
    plt.figure(figsize=(10, 6))

    plt.hist(
        tybyria_scores,
        bins=bins,
        range=(hist_min, hist_max),
        alpha=0.5,
        label="Tybyria",
        density=True,
        edgecolor="black",
    )

    plt.hist(
        vader_scores,
        bins=bins,
        range=(hist_min, hist_max),
        alpha=0.5,
        label="VADER normalizado",
        density=True,
        edgecolor="black",
    )

    plt.xlabel("Score normalizado (0 a 1)")
    plt.ylabel("Densidade")
    plt.title("Distribuição comparativa — Tybyria vs VADER normalizado")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"📊 PNG comparativo: {out_path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    client = storage.Client()

    # TYBYRIA já está em 0..1
    tybyria_scores = collect_scores(
        client=client,
        bucket_name=BUCKET_NAME,
        prefix=PREFIX_TYBYRIA,
        file_suffix=FILE_SUFFIX_TYBYRIA,
        score_col=SCORE_COL_TYBYRIA,
        hist_min=HIST_MIN,
        hist_max=HIST_MAX,
        label="TYBYRIA",
        normalizer=normalize_identity,
        raw_min=0.0,
        raw_max=1.0,
    )

    save_histogram(
        scores=tybyria_scores,
        out_path=os.path.join(OUTPUT_DIR, OUT_PNG_TYBYRIA),
        title="Histograma — Tybyria Score (0 a 1)",
        xlabel=SCORE_COL_TYBYRIA,
        hist_min=HIST_MIN,
        hist_max=HIST_MAX,
        bins=BINS,
    )

    # VADER bruto é -1..1; aqui normalizamos para 0..1
    vader_scores = collect_scores(
        client=client,
        bucket_name=BUCKET_NAME,
        prefix=PREFIX_VADER,
        file_suffix=FILE_SUFFIX_VADER,
        score_col=SCORE_COL_VADER,
        hist_min=HIST_MIN,
        hist_max=HIST_MAX,
        label="VADER_NORMALIZADO",
        normalizer=normalize_vader_to_01,
        raw_min=-1.0,
        raw_max=1.0,
    )

    save_histogram(
        scores=vader_scores,
        out_path=os.path.join(OUTPUT_DIR, OUT_PNG_VADER),
        title="Histograma — VADER Compound normalizado para 0 a 1",
        xlabel="vader_compound_normalized",
        hist_min=HIST_MIN,
        hist_max=HIST_MAX,
        bins=BINS,
    )

    save_comparative_histogram(
        tybyria_scores=tybyria_scores,
        vader_scores=vader_scores,
        out_path=os.path.join(OUTPUT_DIR, OUT_PNG_COMPARE),
        hist_min=HIST_MIN,
        hist_max=HIST_MAX,
        bins=BINS,
    )


if __name__ == "__main__":
    main()
