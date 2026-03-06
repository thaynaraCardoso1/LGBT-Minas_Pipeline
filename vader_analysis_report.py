import csv
import io
from collections import defaultdict
from statistics import mean, stdev
from google.cloud import storage

# =========================
# CONFIG
# =========================
BUCKET = "lgbtminas-dados"
BLOB = "rede social/analysis/vader/VADERZAO.csv"

MG_SUBS = {
    "MinasGerais",
    "BeloHorizonte",
    "montesclaros_",
    "OuroPreto",
    "Uberaba",
    "juizdefora",
    "Uberlandia",
}

BR_SUBS = {
    "brasil",
    "Brazil",
    "Twitter_Brasil",
    "BrasildoB",
    "brasilimpo",
    "brasillivre",
    "SubredditsBrasil",
    "brasil_liberal",
}


# =========================
# LOAD
# =========================
def load_data_by_subreddit():
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(BLOB)

    data = defaultdict(list)

    with blob.open("rb") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
        for row in reader:
            sub = row.get("subreddit")
            if not sub:
                continue

            try:
                score = float(row.get("vader_compound", 0) or 0)
            except:
                score = 0.0

            row["vader_compound"] = score
            data[sub].append(row)

    return data


# =========================
# PRINT HELPERS
# =========================
def text_original_only(row, max_len=180):
    """
    Usa SOMENTE a coluna text_original do VADERZAO.
    - remove quebras de linha pra não bagunçar a impressão
    - colapsa espaços
    - corta em max_len
    """
    txt = row.get("text_original", "")
    if txt is None:
        txt = ""
    txt = txt.replace("\r", " ").replace("\n", " ")
    # colapsa múltiplos espaços
    txt = " ".join(txt.split())
    return txt[:max_len]


# =========================
# REPORTS
# =========================
def top10_by_subreddit(data, allowed_subs, title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

    for sub in sorted(allowed_subs):
        rows = data.get(sub, [])
        if not rows:
            print(f"\n--- {sub} --- (sem dados)")
            continue

        ordered = sorted(rows, key=lambda r: r["vader_compound"], reverse=True)[:10]

        print(f"\n--- {sub} ---")
        for r in ordered:
            print(f"{r['vader_compound']:>7.4f}  {r.get('id','')}  {text_original_only(r)}")


def top10_abs_by_subreddit(data, allowed_subs, title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

    for sub in sorted(allowed_subs):
        rows = data.get(sub, [])
        if not rows:
            print(f"\n--- {sub} --- (sem dados)")
            continue

        ordered = sorted(rows, key=lambda r: abs(r["vader_compound"]), reverse=True)[:10]

        print(f"\n--- {sub} ---")
        for r in ordered:
            print(f"{r['vader_compound']:>7.4f}  {r.get('id','')}  {text_original_only(r)}")


def stats_by_subreddit(data, allowed_subs, title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

    results = []
    for sub in sorted(allowed_subs):
        rows = data.get(sub, [])
        if len(rows) < 2:
            continue

        scores = [r["vader_compound"] for r in rows]
        avg = mean(scores)
        sd = stdev(scores)
        n = len(scores)
        results.append((sub, avg, sd, n))

    results.sort(key=lambda x: x[1], reverse=True)

    for sub, avg, sd, n in results:
        print(f"{sub:20}  média={avg:>8.4f}  sd={sd:>8.4f}  n={n}")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    data = load_data_by_subreddit()

    print("\n##########################")
    print("### GRUPO MG")
    print("##########################")

    top10_by_subreddit(
        data,
        MG_SUBS,
        "TOP 10 (MAIOR vader_compound) POR SUBREDDIT — MG",
    )

    top10_abs_by_subreddit(
        data,
        MG_SUBS,
        "TOP 10 (MAIS EXTREMOS = abs(vader_compound)) POR SUBREDDIT — MG",
    )

    stats_by_subreddit(
        data,
        MG_SUBS,
        "ESTATÍSTICAS (média, sd, N) POR SUBREDDIT — MG",
    )

    print("\n\n##########################")
    print("### GRUPO BRASIL")
    print("##########################")

    top10_by_subreddit(
        data,
        BR_SUBS,
        "TOP 10 (MAIOR vader_compound) POR SUBREDDIT — BRASIL",
    )

    top10_abs_by_subreddit(
        data,
        BR_SUBS,
        "TOP 10 (MAIS EXTREMOS = abs(vader_compound)) POR SUBREDDIT — BRASIL",
    )

    stats_by_subreddit(
        data,
        BR_SUBS,
        "ESTATÍSTICAS (média, sd, N) POR SUBREDDIT — BRASIL",
    )
