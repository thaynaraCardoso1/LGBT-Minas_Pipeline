#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import io
import hashlib
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple

import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
from google.cloud import storage


# ==========================================================
# CONFIG (via env vars ou defaults)
# ==========================================================
MODEL_NAME = os.getenv("TYBYRIA_MODEL_NAME", "Veronyka/tybyria-v2.1")
THRESHOLD = float(os.getenv("TYBYRIA_THRESHOLD", "0.30"))
BATCH_SIZE = int(os.getenv("TYBYRIA_BATCH_SIZE", "32"))
MAX_LEN = int(os.getenv("TYBYRIA_MAX_LEN", "64"))

# Bucket/prefixos
BUCKET_NAME = os.getenv("TYBYRIA_BUCKET", "").strip()
PREFIX_BASE = os.getenv("TYBYRIA_PREFIX_BASE", "rede social").strip()

INPUT_PREFIX = f"{PREFIX_BASE}/processed/"
OUTPUT_PREFIX = f"{PREFIX_BASE}/analysis/"
TMP_PREFIX = f"{PREFIX_BASE}/tmp/"

LOCAL_WORKDIR = os.getenv("TYBYRIA_LOCAL_WORKDIR", "/tmp/tybyria_work").strip()

# checkpoint a cada N linhas processadas
CHECKPOINT_EVERY = int(os.getenv("TYBYRIA_CHECKPOINT_EVERY", "128"))

# Nome das colunas esperadas
TEXT_COL = os.getenv("TYBYRIA_TEXT_COL", "text_original").strip()
SCORE_COL = "tybyria_score"
LABEL_COL = "tybyria_label"


# ==========================================================
# UTIL
# ==========================================================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def die(msg: str, code: int = 1):
    print(f"❌ {msg}", file=sys.stderr)
    sys.exit(code)


def sha1_of_string(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:12]


# ==========================================================
# LIMPEZA
# ==========================================================
def clean_df_for_tybyria(df: pd.DataFrame, text_col: str = TEXT_COL) -> pd.DataFrame:
    """
    Limpa o CSV antes do Tybyria:
    - remove linhas totalmente vazias
    - garante coluna de texto como string
    - trim (strip)
    - remove textos vazios/só espaço
    - remove "nan" que aparece quando converte NaN -> string
    """
    if df is None or df.empty:
        return df

    df = df.dropna(how="all").copy()

    if text_col not in df.columns:
        return df

    # Normaliza texto
    s = df[text_col].astype(str).str.strip()
    s = s.replace("nan", "", regex=False)

    df[text_col] = s

    # Remove linhas com texto vazio
    df = df[df[text_col].notna() & (df[text_col] != "")]
    return df


# ==========================================================
# GCS HELPERS
# ==========================================================
class GCS:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def blob_exists(self, gcs_path: str) -> bool:
        return self.bucket.blob(gcs_path).exists(self.client)

    def download_to(self, gcs_path: str, local_path: str):
        ensure_dir(os.path.dirname(local_path))
        self.bucket.blob(gcs_path).download_to_filename(local_path)

    def upload_from(self, local_path: str, gcs_path: str):
        self.bucket.blob(gcs_path).upload_from_filename(local_path)

    def delete_blob(self, gcs_path: str):
        self.bucket.blob(gcs_path).delete()

    def list_csv(self, prefix: str) -> Iterable[str]:
        for b in self.client.list_blobs(self.bucket_name, prefix=prefix):
            if b.name.endswith(".csv"):
                yield b.name


# ==========================================================
# TYBYRIA INFERENCE
# ==========================================================
@dataclass
class TybyriaRuntime:
    tokenizer: AutoTokenizer
    model: AutoModelForSequenceClassification
    device: torch.device


def load_tybyria_runtime() -> TybyriaRuntime:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"🚀 Dispositivo: {device}")

    print("📦 Carregando modelo/tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME).to(device)
    model.eval()

    return TybyriaRuntime(tokenizer=tokenizer, model=model, device=device)


def infer_batch(rt: TybyriaRuntime, texts: List[str]) -> List[float]:
    inputs = rt.tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LEN,
        return_tensors="pt",
    ).to(rt.device)

    with torch.no_grad():
        outputs = rt.model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=1)
        scores = probs[:, 1].detach().cpu().numpy().tolist()

    return scores


# ==========================================================
# PIPELINE (processed -> clean -> tybyria -> analysis)
# ==========================================================
def output_name_from_input(gcs_in: str) -> str:
    """processed/RC_2025-02_BR.csv -> analysis/RC_2025-02_BR_tybyria.csv"""
    base = os.path.basename(gcs_in)
    return f"{OUTPUT_PREFIX}{base.replace('.csv', '_tybyria.csv')}"


def checkpoint_name_from_input(gcs_in: str) -> str:
    """checkpoint em tmp/ com nome estável"""
    base = os.path.basename(gcs_in)
    return f"{TMP_PREFIX}{base.replace('.csv', '_parcial.csv')}"


def local_paths_for_input(gcs_in: str) -> Tuple[str, str, str, str]:
    """
    Retorna:
    - local_in (baixado)
    - local_clean (limpo)
    - local_tmp (checkpoint)
    - local_out (final)
    """
    base = os.path.basename(gcs_in)
    local_in = os.path.join(LOCAL_WORKDIR, base)
    local_clean = os.path.join(LOCAL_WORKDIR, base.replace(".csv", "_clean.csv"))
    local_tmp = os.path.join(LOCAL_WORKDIR, base.replace(".csv", "_parcial.csv"))
    local_out = os.path.join(LOCAL_WORKDIR, base.replace(".csv", "_tybyria.csv"))
    return local_in, local_clean, local_tmp, local_out


def read_csv_safely(path: str) -> pd.DataFrame:
    # dtype=str + keep_default_na=False evita NaNs chatos e preserva strings
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=True,
    )


def write_csv(df: pd.DataFrame, path: str):
    ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=False)


def compute_start_idx_from_checkpoint(df_ckpt: pd.DataFrame) -> int:
    if df_ckpt is None or df_ckpt.empty:
        return 0
    if SCORE_COL not in df_ckpt.columns:
        return 0
    # conta quantos já têm score preenchido
    # como dtype=str pode vir string vazia, então normaliza
    filled = df_ckpt[SCORE_COL].astype(str).str.strip()
    return int((filled != "").sum())


def process_one_file(
    gcs: GCS,
    rt: TybyriaRuntime,
    gcs_in: str,
    delete_checkpoint_after: bool = True,
):
    name = os.path.basename(gcs_in)

    gcs_out = output_name_from_input(gcs_in)
    gcs_tmp = checkpoint_name_from_input(gcs_in)

    local_in, local_clean, local_tmp, local_out = local_paths_for_input(gcs_in)

    print(f"\n🧩 Arquivo: {name}")
    print(f"   • IN : gs://{BUCKET_NAME}/{gcs_in}")
    print(f"   • OUT: gs://{BUCKET_NAME}/{gcs_out}")

    # 1) Baixa input
    print(f"⬇️  Baixando processed...")
    gcs.download_to(gcs_in, local_in)

    # 2) Tenta retomar checkpoint
    df_proc: Optional[pd.DataFrame] = None
    start_idx = 0

    if gcs.blob_exists(gcs_tmp):
        print(f"🔄 Checkpoint encontrado no GCS: {gcs_tmp}")
        try:
            gcs.download_to(gcs_tmp, local_tmp)
            df_proc = read_csv_safely(local_tmp)
            start_idx = compute_start_idx_from_checkpoint(df_proc)
            print(f"🔁 Retomando da linha {start_idx}")
        except Exception as e:
            print(f"⚠️  Falha ao ler checkpoint ({e}). Vou recomeçar do zero.")
            df_proc = None
            start_idx = 0

    # 3) Se não retomou, inicia do zero: lê, limpa, cria colunas
    if df_proc is None or start_idx == 0:
        df_original = read_csv_safely(local_in)

        if TEXT_COL not in df_original.columns:
            print(f"⚠️  {name} não tem coluna '{TEXT_COL}'. Pulando.")
            return

        # ✅ limpeza
        df_clean = clean_df_for_tybyria(df_original, TEXT_COL)
        if df_clean is None or df_clean.empty:
            print(f"⚠️  {name}: vazio após limpeza. Pulando.")
            return

        # opcional: salvar o "clean" local pra debug
        write_csv(df_clean, local_clean)

        df_proc = df_clean.copy()
        df_proc[SCORE_COL] = ""
        df_proc[LABEL_COL] = ""
        start_idx = 0

    # 4) roda batches
    texts = df_proc[TEXT_COL].astype(str).tolist()
    total = len(texts)
    if total == 0:
        print(f"⚠️  {name}: sem textos após limpeza. Pulando.")
        return

    pbar = tqdm(range(start_idx, total, BATCH_SIZE), desc=f"Analisando {name}")

    for i in pbar:
        end = min(i + BATCH_SIZE, total)
        batch_texts = texts[i:end]

        scores = infer_batch(rt, batch_texts)

        # escreve nos índices corretos do df (preserva index original)
        idx_slice = df_proc.index[i:end]
        df_proc.loc[idx_slice, SCORE_COL] = [str(x) for x in scores]
        df_proc.loc[idx_slice, LABEL_COL] = ["1" if x >= THRESHOLD else "0" for x in scores]

        # checkpoint
        if CHECKPOINT_EVERY > 0 and (i > start_idx) and (i % CHECKPOINT_EVERY == 0):
            write_csv(df_proc, local_tmp)
            gcs.upload_from(local_tmp, gcs_tmp)
            tqdm.write(f"💾 Checkpoint salvo: linha {i}/{total}")

    # 5) finaliza
    write_csv(df_proc, local_out)
    gcs.upload_from(local_out, gcs_out)
    print(f"✅ Finalizado: gs://{BUCKET_NAME}/{gcs_out}")

    # 6) remove checkpoint (opcional)
    if delete_checkpoint_after and gcs.blob_exists(gcs_tmp):
        try:
            gcs.delete_blob(gcs_tmp)
        except Exception as e:
            print(f"⚠️  Não consegui deletar checkpoint {gcs_tmp}: {e}")

    # 7) limpa local (opcional)
    for p in (local_in, local_clean, local_tmp, local_out):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


# ==========================================================
# MAIN
# ==========================================================
def main():
    if not BUCKET_NAME:
        die("Defina o bucket: export TYBYRIA_BUCKET='lgbtminas-dados' (ou edite BUCKET_NAME).")

    ensure_dir(LOCAL_WORKDIR)

    gcs = GCS(BUCKET_NAME)
    rt = load_tybyria_runtime()

    # 1) lista processed e analysis
    processed_files = sorted(list(gcs.list_csv(INPUT_PREFIX)))
    if not processed_files:
        die(f"Nenhum CSV encontrado em gs://{BUCKET_NAME}/{INPUT_PREFIX}")

    existing_outputs: Set[str] = set(gcs.list_csv(OUTPUT_PREFIX))

    # 2) filtra: processed que NÃO tem output final em analysis
    to_process: List[str] = []
    for gcs_in in processed_files:
        gcs_out = output_name_from_input(gcs_in)
        if gcs_out not in existing_outputs:
            to_process.append(gcs_in)

    print(f"🧾 Encontrados {len(processed_files)} arquivos em {INPUT_PREFIX}")
    print(f"🧪 Pendentes (processed sem output em analysis): {len(to_process)}")

    if not to_process:
        print("✨ Nada a fazer. Tudo em processed já tem output em analysis.")
        return

    # 3) processa pendentes
    for gcs_in in to_process:
        # segurança: se por algum motivo apareceu output enquanto você roda, pula
        gcs_out = output_name_from_input(gcs_in)
        if gcs.blob_exists(gcs_out):
            print(f"⏭️  Já finalizado durante a execução: {os.path.basename(gcs_in)}")
            continue

        try:
            process_one_file(gcs, rt, gcs_in, delete_checkpoint_after=True)
        except KeyboardInterrupt:
            print("\n⛔ Interrompido pelo usuário (Ctrl+C).")
            raise
        except Exception as e:
            print(f"❌ Erro processando {os.path.basename(gcs_in)}: {e}", file=sys.stderr)
            # continua para o próximo
            continue

    print("\n✨ Análise concluída!")


if __name__ == "__main__":
    main()

