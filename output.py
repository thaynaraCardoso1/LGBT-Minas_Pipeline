import os
import pandas as pd

BASE_DIR = os.path.abspath(".")  # se você estiver na pasta LGBT+Minas
csv_path = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "analysis",
    "RC_2025-05_comments_BR_sentiment.csv"
)

df = pd.read_csv(csv_path)

print("Colunas:", df.columns)
print(df["polarity"].head(10))
print(df["polarity"].describe())
print("Valores menores que -1:")
print(df[df["polarity"] < -1][["text", "polarity"]].head(20))
