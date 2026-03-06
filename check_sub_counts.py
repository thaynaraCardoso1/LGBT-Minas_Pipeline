import csv, io
from collections import Counter
from google.cloud import storage

BUCKET="lgbtminas-dados"
BLOB="rede social/analysis/vader/VADERZAO.csv"

TARGET = {
 "MinasGerais","BeloHorizonte","montesclaros_","OuroPreto","Uberaba","juizdefora","Uberlandia",
 "brasil","Brazil","Twitter_Brasil","BrasildoB","brasilimpo","brasillivre","SubredditsBrasil","brasil_liberal"
}

c = Counter()
client=storage.Client()
blob=client.bucket(BUCKET).blob(BLOB)

with blob.open("rb") as f:
    r = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
    for row in r:
        s = row.get("subreddit","")
        if s in TARGET:
            c[s]+=1

for k,v in c.most_common():
    print(f"{k}: {v}")
