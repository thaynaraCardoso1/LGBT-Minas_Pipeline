import os
import pandas as pd
import glob
import subprocess
from tqdm import tqdm
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Configurações de Caminhos
BASE_PATH = 'bases/rede social/reddit/'
INPUT_DIR = os.path.join(BASE_PATH, 'processed/')
OUTPUT_DIR = os.path.join(BASE_PATH, 'analysis/')
TMP_DIR = os.path.join(BASE_PATH, 'tmp/')

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Tentar impedir o Mac de dormir (caffeinate)
subprocess.Popen(["caffeinate", "-i"])

def alertar(mensagem):
    """ Faz o Mac falar e dar um bip """
    try:
        os.system(f'say "{mensagem}"')
        os.system('echo -e "\a"')
    except:
        pass

# Inicializa o analisador VADER
analyzer = SentimentIntensityAnalyzer()

BATCH_SIZE = 128

# 2. Processamento com Checkpoint Ultra-Frequente
arquivos = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv")))

for file_path in arquivos:
    name = os.path.basename(file_path)
    output_path = os.path.join(OUTPUT_DIR, name.replace(".csv", "_vader.csv"))
    tmp_path = os.path.join(TMP_DIR, name.replace(".csv", "_parcial_vader.csv"))

    if os.path.exists(output_path):
        print(f"⏭️  {name} já finalizado.")
        continue

    if os.path.exists(tmp_path):
        try:
            df_proc = pd.read_csv(tmp_path)
            start_idx = df_proc['vader_compound'].notna().sum()
            print(f"🔄 Checkpoint encontrado! Retomando {name} da linha {start_idx}")
        except Exception as e:
            print(f"⚠️ Erro ao ler checkpoint, começando do zero: {e}")
            df_original = pd.read_csv(file_path)
            df_proc = df_original.dropna(subset=['text_original']).copy()
            start_idx = 0
    else:
        df_original = pd.read_csv(file_path)
        df_proc = df_original.dropna(subset=['text_original']).copy()
        start_idx = 0

    # Inicializa as colunas VADER (se não existirem ainda)
    if 'vader_compound' not in df_proc.columns:
        df_proc['vader_compound'] = None
        df_proc['vader_label'] = None

    texts = df_proc['text_original'].astype(str).tolist()

    for i in tqdm(range(start_idx, len(texts), BATCH_SIZE), desc=f"Analisando {name}"):
        end_idx = min(i + BATCH_SIZE, len(texts))
        batch_texts = texts[i:end_idx]

        compound_scores = []
        labels = []

        for text in batch_texts:
            score = analyzer.polarity_scores(text)
            compound = score['compound']
            compound_scores.append(compound)

            if compound >= 0.05:
                labels.append('positive')
            elif compound <= -0.05:
                labels.append('negative')
            else:
                labels.append('neutral')

        current_indices = df_proc.index[i:end_idx]
        df_proc.loc[current_indices, 'vader_compound'] = compound_scores
        df_proc.loc[current_indices, 'vader_label'] = labels

        if (i > start_idx) and (i % 512 == 0):
            df_proc.to_csv(tmp_path, index=False)
            tqdm.write(f"💾 Checkpoint gravado: linha {i}/{len(texts)}")

    df_proc.to_csv(output_path, index=False)
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    print(f"✅ Arquivo finalizado: {output_path}")
    alertar(f"Concluído arquivo {name}")

alertar("Todos os processos foram finalizados.")
print("\n✨ Análise com VADER concluída!")
