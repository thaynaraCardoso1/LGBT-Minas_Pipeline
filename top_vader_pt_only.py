import csv, io, re
from google.cloud import storage

BUCKET="lgbtminas-dados"
BLOB="rede social/analysis/vader/VADERZAO.csv"

MG_SUBS = {"MinasGerais","BeloHorizonte","montesclaros_","OuroPreto","Uberaba","juizdefora","Uberlandia"}
BR_SUBS = {"brasil","Brazil","Twitter_Brasil","BrasildoB","brasilimpo","brasillivre","SubredditsBrasil","brasil_liberal"}

PT_MARKS = re.compile(r"[ãõçáéíóúàêôâ]", re.IGNORECASE)
PT_WORDS = re.compile(r"\b(que|não|pra|para|com|você|vocês|isso|essa|esse|tá|está|também|porque|muito|mais|menos|aqui|lá)\b", re.IGNORECASE)

def looks_pt(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    # remove spam repetitivo tipo "L L L L" ou "yes yes yes"
    if len(t) > 30 and len(set(t.split())) <= 2:
        return False
    score = 0
    if PT_MARKS.search(t):
        score += 2
    if PT_WORDS.search(t):
        score += 1
    return score >= 2  # exige pelo menos "cara de PT"

def load_filtered(allowed_subs):
    client=storage.Client()
    blob=client.bucket(BUCKET).blob(BLOB)
    rows=[]
    with blob.open("rb") as f:
        r = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
        for row in r:
            if row.get("subreddit") not in allowed_subs:
                continue
            txt = row.get("text_original") or row.get("text_clean") or ""
            if not looks_pt(txt):
                continue
            try:
                row["vader_compound"] = float(row.get("vader_compound", "0") or 0)
            except:
                row["vader_compound"] = 0.0
            rows.append(row)
    return rows

def print_top(rows, title):
    rows = sorted(rows, key=lambda x: x["vader_compound"], reverse=True)[:10]
    print("\n" + "="*80)
    print(title)
    print("="*80)
    for r in rows:
        txt = (r.get("text_original") or "").replace("\n"," ")
        txt = re.sub(r"\s+", " ", txt)[:140]
        print(f"{r['subreddit']:18} {r['vader_compound']:>7}  {r['id']}  {txt}")

if __name__ == "__main__":
    mg = load_filtered(MG_SUBS)
    br = load_filtered(BR_SUBS)
    print_top(mg, "TOP 10 (PT-only) - MG")
    print_top(br, "TOP 10 (PT-only) - BRASIL")
