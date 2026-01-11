import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix
import joblib


# Descobre a raiz do projeto (pasta LGBT+Minas)
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

# 📥 ARQUIVO DE ENTRADA:
# Espera um CSV com pelo menos:
# - coluna "text"  -> conteúdo textual
# - coluna "label" -> 0 (negativo) / 1 (positivo)
INPUT_CSV = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "processed",
    "RC_2025-05_comments_BR.csv",  # ajuste o nome se for outro

)

# Diretório para salvar modelo e saídas
OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "bases", "rede social", "reddit", "analysis",
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(
    OUTPUT_DIR,
    "RC_2025-05_comments_BR_rotulado_tfidf_sentiment.csv",
)

MODEL_PATH = os.path.join(
    OUTPUT_DIR,
    "tfidf_logreg_sentiment.joblib",
)


def main():
    print("📂 BASE_DIR :", BASE_DIR)
    print("📥 INPUT    :", INPUT_CSV)
    print("📤 OUTPUT   :", OUTPUT_CSV)
    print("💾 MODELO   :", MODEL_PATH)

    if not os.path.exists(INPUT_CSV):
        print("❌ ERRO: arquivo de entrada não existe.")
        return

    # 1) Carrega dados
    print("🔍 Lendo CSV rotulado...")
    df = pd.read_csv(INPUT_CSV)

    print(f"✅ CSV lido com {len(df):,} linhas")
    print("📑 Colunas disponíveis:", list(df.columns))

    if "text" not in df.columns:
        print("❌ ERRO: não encontrei coluna 'text'.")
        return

    if "label" not in df.columns:
        print("❌ ERRO: não encontrei coluna 'label'.")
        print("   Crie uma coluna 'label' com 0 = negativo, 1 = positivo.")
        return

    # Remove linhas sem texto ou rótulo
    df = df.dropna(subset=["text", "label"])
    df["label"] = df["label"].astype(int)

    print(f"📉 Após limpeza: {len(df):,} linhas")

    X = df["text"].astype(str)
    y = df["label"]

    # 2) Split treino/teste
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    print(f"🧪 Treino: {len(X_train):,} | Teste: {len(X_test):,}")

    # 3) Pipeline TF-IDF + Logistic Regression
    # Configuração alinhada com a RS:
    # - max_features=10000
    # - ngram_range=(1,2)
    # - LogisticRegression para binário
    pipeline = Pipeline([
        (
            "tfidf",
            TfidfVectorizer(
                max_features=10000,
                ngram_range=(1, 2),
                min_df=5,
            ),
        ),
        (
            "clf",
            LogisticRegression(
                max_iter=1000,
                n_jobs=-1,
                solver="liblinear",  # bom para binário, robusto
            ),
        ),
    ])

    # 4) Treino
    print("🧠 Treinando modelo TF-IDF + LogisticRegression...")
    pipeline.fit(X_train, y_train)

    # 5) Avaliação
    print("📊 Avaliando no conjunto de teste...")
    y_pred = pipeline.predict(X_test)

    print("\n===== CLASSIFICATION REPORT =====")
    print(classification_report(y_test, y_pred, digits=4))

    print("===== MATRIZ DE CONFUSÃO =====")
    print(confusion_matrix(y_test, y_pred))

    # 6) Salva modelo
    joblib.dump(pipeline, MODEL_PATH)
    print(f"💾 Modelo salvo em: {MODEL_PATH}")

    # 7) Salva CSV com predição (para facilitar análise posterior)
    df_test = pd.DataFrame({
        "text": X_test,
        "label_true": y_test,
        "label_pred": y_pred,
    })

    df_test.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Predições salvas em: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
