import os
import pandas as pd

BASE_DIR = "/Users/tata/Documents/Documentos - Mac mini de Thaynara/LGBT+Minas"

csv_sent = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "analysis",
    "RC_2025-05_comments_BR_sentiment_nlptown_bert_multi_5sentiment.csv",
)

df = pd.read_csv(csv_sent)

# 5 exemplos bem negativos
print("\n==== MUITO NEGATIVOS ====")
for t in df[df["sentiment_label"] == "very_negative"]["text"].head(5):
    print("-", t[:300].replace("\n", " "), "\n")

# 5 exemplos bem positivos
print("\n==== MUITO POSITIVOS ====")
for t in df[df["sentiment_label"] == "very_positive"]["text"].head(5):
    print("-", t[:300].replace("\n", " "), "\n")
