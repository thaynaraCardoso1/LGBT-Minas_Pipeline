import os
import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F


# ====== CONFIG GERAL ======

# raiz do projeto (LGBT+Minas)
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

# CSV de entrada: comentários BR (sem rótulo)
INPUT_CSV = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "processed",
    "RC_2025-04_BR.csv",
)

# Modelo de sentimento (BERT multilingual 5 classes)
MODEL_NAME = "nlptown/bert-base-multilingual-uncased-sentiment"
MODEL_TAG = "nlptown_bert_multi_5sentiment"  # usado no nome do arquivo

# diretório para salvar análise
OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "analysis",
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(
    OUTPUT_DIR,
    f"RC_2025-05_comments_BR_sentiment_{MODEL_TAG}.csv",
)


def main():
    print("📂 BASE_DIR :", BASE_DIR)
    print("📥 INPUT    :", INPUT_CSV)
    print("📤 OUTPUT   :", OUTPUT_CSV)
    print("🧠 MODEL    :", MODEL_NAME)

    if not os.path.exists(INPUT_CSV):
        print("❌ ERRO: arquivo de entrada não existe.")
        return

    # 1) Carrega dados
    print("🔍 Lendo CSV de comentários BR...")
    df = pd.read_csv(INPUT_CSV)

    if "text" not in df.columns:
        print("❌ ERRO: não encontrei coluna 'text' no CSV.")
        return

    print(f"✅ Linhas totais: {len(df):,}")

    # drop textos vazios
    df = df.dropna(subset=["text"]).copy()
    df["text"] = df["text"].astype(str)

    print(f"📉 Após remoção de textos vazios: {len(df):,}")

    # 2) Carrega modelo + tokenizer
    print("🧠 Carregando tokenizer e modelo BERT (nlptown 5-class)...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()
    print(f"💻 Usando device: {device}")

    # Saída do modelo:
    # 0 -> 1 star (very negative)
    # 1 -> 2 stars (negative)
    # 2 -> 3 stars (neutral)
    # 3 -> 4 stars (positive)
    # 4 -> 5 stars (very positive)
    idx2stars = {
        0: 1,
        1: 2,
        2: 3,
        3: 4,
        4: 5,
    }

    def stars_to_label(stars: int) -> str:
        if stars <= 1:
            return "very_negative"
        elif stars == 2:
            return "negative"
        elif stars == 3:
            return "neutral"
        elif stars == 4:
            return "positive"
        else:
            return "very_positive"

    def stars_to_polarity(stars: int) -> float:
        """
        Mapeia:
        1★ -> -1.0
        2★ -> -0.5
        3★ ->  0.0
        4★ -> +0.5
        5★ -> +1.0
        """
        return (stars - 3) / 2.0

    # 3) Processa em batch
    batch_size = 32
    texts = df["text"].tolist()

    all_stars = []
    all_labels = []
    all_polarity = []

    all_p_s1 = []
    all_p_s2 = []
    all_p_s3 = []
    all_p_s4 = []
    all_p_s5 = []

    print("⚙️ Calculando sentimento (BERT - nlptown 5 classes)...")

    for i in tqdm(range(0, len(texts), batch_size)):
        batch_texts = texts[i: i + batch_size]

        encodings = tokenizer(
            batch_texts,
            padding=True,
            truncation=True,
            max_length=256,
            return_tensors="pt",
        )
        encodings = {k: v.to(device) for k, v in encodings.items()}

        with torch.no_grad():
            outputs = model(**encodings)
            logits = outputs.logits
            probs = F.softmax(logits, dim=-1)  # [batch, 5]

        preds = torch.argmax(probs, dim=-1).cpu().numpy()
        probs_np = probs.cpu().numpy()

        p_s1 = probs_np[:, 0]
        p_s2 = probs_np[:, 1]
        p_s3 = probs_np[:, 2]
        p_s4 = probs_np[:, 3]
        p_s5 = probs_np[:, 4]

        batch_stars = [idx2stars[int(idx)] for idx in preds]
        batch_labels = [stars_to_label(s) for s in batch_stars]
        batch_polarity = [stars_to_polarity(s) for s in batch_stars]

        all_stars.extend(batch_stars)
        all_labels.extend(batch_labels)
        all_polarity.extend(batch_polarity)

        all_p_s1.extend(p_s1)
        all_p_s2.extend(p_s2)
        all_p_s3.extend(p_s3)
        all_p_s4.extend(p_s4)
        all_p_s5.extend(p_s5)

    # 4) Anexa ao DataFrame
    df["sentiment_stars"] = all_stars              # 1 a 5
    df["sentiment_label"] = all_labels            # very_negative, ..., very_positive
    df["polarity"] = all_polarity                 # -1.0 a +1.0

    df["p_star_1"] = all_p_s1
    df["p_star_2"] = all_p_s2
    df["p_star_3"] = all_p_s3
    df["p_star_4"] = all_p_s4
    df["p_star_5"] = all_p_s5

    # 5) Salva saída
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Arquivo salvo em: {OUTPUT_CSV}")

    # 6) Pequeno resumo
    print("\n📊 Distribuição de estrelas:")
    print(df["sentiment_stars"].value_counts().sort_index())

    print("\n📊 Distribuição de rótulos (proporção):")
    print(df["sentiment_label"].value_counts(normalize=True).rename("proporção"))

    print("\n📊 Distribuição de rótulos (absoluto):")
    print(df["sentiment_label"].value_counts())


if __name__ == "__main__":
    main()
