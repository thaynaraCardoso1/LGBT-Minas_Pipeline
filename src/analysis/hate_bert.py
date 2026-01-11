import os
import pandas as pd
from pysentimiento import create_analyzer
from src.utils.logger import setup_logger
from src.analysis.config import BASE_DIR

INPUT_FILE = os.path.join(BASE_DIR, "bases", "rede social", "reddit", "processed", "RC_2025-05_comments_BR.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "bases", "rede social", "reddit", "analysis", "RC_2025-05_comments_BR_hatebert.csv")

def main():
    logger = setup_logger("logs/hatebert_analysis.log")
    logger.info("🔍 Iniciando análise de discurso de ódio com modelo BERT...")

    df = pd.read_csv(INPUT_FILE)
    df = df[df["text"].notnull()]

    logger.info(f"📄 Total de textos: {len(df)}")

    # Você pode mudar "multilingual" para "spanish" ou "english" se for usar outro idioma
    analyzer = create_analyzer(task="hate_speech", lang="multilingual")

    results = df["text"].apply(lambda x: analyzer.predict(x))
    df["hate_label"] = results.apply(lambda r: r.output)
    df["hate_probs"] = results.apply(lambda r: r.probas)

    logger.info("✅ Análise concluída, salvando...")

    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"💾 Salvo em: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
