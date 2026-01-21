import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os
import glob
import subprocess
from tqdm import tqdm

# 1. Configurações de Caminhos
BASE_PATH = 'bases/rede social/reddit/'
INPUT_DIR = os.path.join(BASE_PATH, 'processed/')
OUTPUT_DIR = os.path.join(BASE_PATH, 'analysis/')
TMP_DIR = os.path.join(BASE_PATH, 'tmp/')
MODEL_NAME = "Veronyka/tybyria-v2.1"
THRESHOLD = 0.30
BATCH_SIZE = 32

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Tentar impedir o Mac de dormir (caffeinate)
subprocess.Popen(["caffeinate", "-i"])

def alertar(mensagem):
    """ Faz o Mac falar e dar um bip """
    try:
        os.system(f'say "{mensagem}"')
        os.system('echo -e "\a"') # Bip do sistema
    except:
        pass

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print(f"🚀 Dispositivo: {device}")

# 2. Carregar Modelo
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME).to(device)
model.eval()

# 3. Processamento com Checkpoint Ultra-Frequente
arquivos = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv")))

for file_path in arquivos:
    name = os.path.basename(file_path)
    output_path = os.path.join(OUTPUT_DIR, name.replace(".csv", "_tybyria.csv"))
    tmp_path = os.path.join(TMP_DIR, name.replace(".csv", "_parcial.csv"))
    
    if os.path.exists(output_path):
        print(f"⏭️  {name} já finalizado.")
        continue

    # Tenta retomar do TMP
    if os.path.exists(tmp_path):
        try:
            df_proc = pd.read_csv(tmp_path)
            # Conta as linhas onde tybyria_score não é nulo
            start_idx = df_proc['tybyria_score'].notna().sum()
            print(f"🔄 Checkpoint encontrado! Retomando {name} da linha {start_idx}")
        except Exception as e:
            print(f"⚠️ Erro ao ler checkpoint, começando do zero: {e}")
            df_original = pd.read_csv(file_path)
            df_proc = df_original.dropna(subset=['text_original']).copy()
            df_proc['tybyria_score'] = None
            df_proc['tybyria_label'] = None
            start_idx = 0
    else:
        df_original = pd.read_csv(file_path)
        df_proc = df_original.dropna(subset=['text_original']).copy()
        df_proc['tybyria_score'] = None
        df_proc['tybyria_label'] = None
        start_idx = 0

    texts = df_proc['text_original'].astype(str).tolist()
    
    # Processamento
    for i in tqdm(range(start_idx, len(texts), BATCH_SIZE), desc=f"Analisando {name}"):
        end_idx = min(i + BATCH_SIZE, len(texts))
        batch_texts = texts[i:end_idx]
        
        inputs = tokenizer(batch_texts, padding=True, truncation=True, max_length=64, return_tensors='pt').to(device)
        
        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=1)
            batch_scores = probs[:, 1].cpu().numpy()
            
            # Localiza os índices corretos para salvar
            current_indices = df_proc.index[i:end_idx]
            df_proc.loc[current_indices, 'tybyria_score'] = batch_scores
            df_proc.loc[current_indices, 'tybyria_label'] = [1 if s >= THRESHOLD else 0 for s in batch_scores]
        
        # --- SALVAMENTO AGRESSIVO (A CADA 100 LINHAS) ---
        # Como o batch é 32, a cada 4 batches ele salva (aprox 128 linhas)
        if (i > start_idx) and (i % 128 == 0):
            df_proc.to_csv(tmp_path, index=False)
            # O print abaixo confirma que o arquivo está sendo gravado no disco
            tqdm.write(f"💾 Checkpoint gravado: linha {i}/{len(texts)}")

    # Finalização
    df_proc.to_csv(output_path, index=False)
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    
    print(f"✅ Arquivo finalizado: {output_path}")
    alertar(f"Concluído arquivo {name}")

alertar("Todos os processos foram finalizados. Excelente trabalho.")
print("\n✨ Análise concluída!")