import csv
import io
from google.cloud import storage

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

def load_rows():
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(BLOB)

    rows = []
    with blob.open("rb") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
        for row in reader:
            try:
                row["vader_compound"] = float(row.get("vader_compound", 0))
            except:
                row["vader_compound"] = 0.0
            rows.append(row)
    return rows

def print_top(rows, allowed_subs, title):
    filtered = [
        r for r in rows
        if r.get("subreddit") in allowed_subs
    ]

    ordered = sorted(
        filtered,
        key=lambda x: x["vader_compound"],
        reverse=True
    )

    print("\n" + "="*80)
    print(title)
    print("="*80)

    for r in ordered[:10]:
        print(
            f"{r['subreddit']:20} "
            f"{r['vader_compound']:>7}  "
            f"{r['id']}  "
            f"{r['text_original'][:120].replace(chr(10),' ')}"
        )

if __name__ == "__main__":
    rows = load_rows()
    print_top(rows, MG_SUBS, "TOP 10 - SUBREDDITS MG")
    print_top(rows, BR_SUBS, "TOP 10 - SUBREDDITS BRASIL")
