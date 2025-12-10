# src/analysis/sentiment.py

import os
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Descobre a raiz do projeto (LGBT+Minas)
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

INPUT_CSV = os.path.join(
    BASE_DIR,
    "bases/rede social/reddit/processed/RC_2025-05_comments_BR.csv"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "bases/rede social/reddit/analysis"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(
    OUTPUT_DIR,
    "RC_2025-05_comments_BR_sentiment.csv"
)

print(f"Lendo: {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV)

analyzer = SentimentIntensityAnalyzer()

df["polarity"] = df["text"].astype(str).apply(
    lambda x: analyzer.polarity_scores(x)["compound"]
)

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"✅ Sentimento salvo em: {OUTPUT_CSV}")
