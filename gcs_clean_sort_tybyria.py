#!/usr/bin/env python3
import argparse
import csv
import os
import re
import tempfile
import heapq
from typing import List, Dict, Tuple, Optional

import pandas as pd
from google.cloud import storage

BLANK_LINE_RE = re.compile(r'^[\s,;"]*$')

def ensure_trailing_slash(p: str) -> str:
    return p if p.endswith("/") else p + "/"

def build_out_name(out_prefix: str, in_blob_name: str, suffix: str) -> str:
    filename = in_blob_name.split("/")[-1]
    if filename.lower().endswith(".csv"):
        base = filename[:-4]
        return f"{ensure_trailing_slash(out_prefix)}{base}{suffix}.csv"
    return f"{ensure_trailing_slash(out_prefix)}{filename}{suffix}"

def find_text_col(cols: List[str]) -> Optional[str]:
    # você escreveu text_origina, mas vou suportar os dois
    for c in cols:
        if c == "text_original":
            return c
    for c in cols:
        if c == "text_origina":
            return c
    # tenta match case-insensitive
    lower = {c.lower(): c for c in cols}
    if "text_original" in lower:
        return lower["text_original"]
    if "text_origina" in lower:
        return lower["text_origina"]
    return None

def download_blob_to_file(bucket_name: str, blob_name: str, local_path: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists(client):
        raise FileNotFoundError(f"Não achei: gs://{bucket_name}/{blob_name}")
    blob.download_to_filename(local_path)

def upload_file_to_blob(bucket_name: str, blob_name: str, local_path: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

def list_target_blobs(bucket_name: str, prefix: str, pattern: Optional[str]):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    if pattern:
        try:
            rx = re.compile(pattern)
            for b in blobs:
                if rx.search(b.name):
                    yield b.name
        except re.error:
            for b in blobs:
                if pattern in b.name:
                    yield b.name
    else:
        for b in blobs:
            if b.name.lower().endswith(".csv"):
                yield b.name

def is_effectively_blank_row(row: Dict[str, str]) -> bool:
    # Se todas as colunas vierem vazias/None/espaco, considera "linha em branco"
    # (isso pega casos tipo ";;;;" que viram colunas vazias)
    for v in row.values():
        if v is None:
            continue
        if str(v).strip() != "":
            return False
    return True

def external_sort_by_score(
    in_csv_path: str,
    out_csv_path: str,
    sep: str,
    score_col: str,
    text_col: Optional[str],
    chunksize: int,
) -> List[Tuple[float, str]]:
    """
    Retorna lista top10 (score, text) já ordenada desc.
    """
    tmp_dir = tempfile.mkdtemp(prefix="tybyria_sort_")
    chunk_files: List[str] = []

    top_candidates = pd.DataFrame(columns=[score_col] + ([text_col] if text_col else []))

    # Leitura em chunks com pandas
    reader = pd.read_csv(
        in_csv_path,
        sep=sep,
        dtype=str,                 # mantém tudo como string; score convertemos depois
        chunksize=chunksize,
        engine="python",
        on_bad_lines="skip",
        keep_default_na=False,     # evita transformar "" em NaN automaticamente
        skip_blank_lines=True,
    )

    for i, chunk in enumerate(reader):
        # remove linhas totalmente vazias (todas colunas "")
        # (pandas não pega perfeitamente o caso ";;;;", então filtramos manual)
        # também remove linhas que são literalmente em branco no arquivo
        # (por garantia)
        # normaliza: strip em tudo
        for c in chunk.columns:
            chunk[c] = chunk[c].astype(str).map(lambda x: x.strip())

        # drop linhas onde todas colunas estão vazias
        mask_all_empty = (chunk.apply(lambda col: col.eq("")).all(axis=1))
        chunk = chunk[~mask_all_empty].copy()

        if chunk.empty:
            continue

        # score numérico
        chunk[score_col] = pd.to_numeric(chunk[score_col], errors="coerce")
        chunk = chunk.dropna(subset=[score_col])

        if chunk.empty:
            continue

        # ordena desc
        chunk = chunk.sort_values(score_col, ascending=False)

        # atualiza top10
        if text_col and text_col in chunk.columns:
            cand = chunk[[score_col, text_col]].head(50)  # pega um pouco a mais por chunk
            top_candidates = pd.concat([top_candidates, cand], ignore_index=True)
            top_candidates[score_col] = pd.to_numeric(top_candidates[score_col], errors="coerce")
            top_candidates = top_candidates.dropna(subset=[score_col])
            top_candidates = top_candidates.sort_values(score_col, ascending=False).head(10)

        # salva chunk ordenado em arquivo temporário
        chunk_path = os.path.join(tmp_dir, f"chunk_{i:05d}.csv")
        chunk.to_csv(chunk_path, index=False, sep=sep, quoting=csv.QUOTE_MINIMAL)
        chunk_files.append(chunk_path)

    # Se nada pra ordenar
    if not chunk_files:
        # cria arquivo vazio com header mínimo?
        with open(out_csv_path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return []

    # Merge externo (k-way merge) lendo os chunks já ordenados
    # Usa DictReader + heap pela chave -score (desc)
    readers = []
    files = []
    heap = []

    # abre todos os chunks e prepara heap com primeiro item de cada
    for idx, path in enumerate(chunk_files):
        f = open(path, "r", encoding="utf-8", newline="")
        files.append(f)
        dr = csv.DictReader(f, delimiter=sep)
        readers.append(dr)

        try:
            row = next(dr)
        except StopIteration:
            continue

        # pula linhas “em branco”
        while row is not None and is_effectively_blank_row(row):
            try:
                row = next(dr)
            except StopIteration:
                row = None
                break
        if row is None:
            continue

        try:
            score = float(row.get(score_col, "nan"))
        except ValueError:
            score = float("nan")

        if score == score:  # not NaN
            heapq.heappush(heap, (-score, idx, row))

    # fieldnames: usa do primeiro reader (todos iguais por causa do pandas)
    fieldnames = readers[0].fieldnames

    with open(out_csv_path, "w", encoding="utf-8", newline="") as out_f:
        dw = csv.DictWriter(out_f, fieldnames=fieldnames, delimiter=sep, quoting=csv.QUOTE_MINIMAL)
        dw.writeheader()

        while heap:
            neg_score, idx, row = heapq.heappop(heap)
            dw.writerow(row)

            # lê próximo do mesmo chunk
            dr = readers[idx]
            try:
                nxt = next(dr)
            except StopIteration:
                nxt = None

            while nxt is not None and is_effectively_blank_row(nxt):
                try:
                    nxt = next(dr)
                except StopIteration:
                    nxt = None
                    break

            if nxt is None:
                continue

            try:
                s = float(nxt.get(score_col, "nan"))
            except ValueError:
                s = float("nan")

            if s == s:
                heapq.heappush(heap, (-s, idx, nxt))

    # fecha arquivos
    for f in files:
        try:
            f.close()
        except Exception:
            pass

    # top10 final
    result = []
    if not top_candidates.empty and text_col and text_col in top_candidates.columns:
        top_candidates = top_candidates.sort_values(score_col, ascending=False)
        for _, r in top_candidates.iterrows():
            result.append((float(r[score_col]), str(r[text_col])))

    return result

def main():
    ap = argparse.ArgumentParser(description="Limpa linhas em branco, ordena DESC por tybyria_score e grava CSV novo no GCS.")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True, help='Prefixo de ENTRADA (ex: "rede social/analysis/")')
    ap.add_argument("--out-prefix", required=True, help='Prefixo de SAÍDA (ex: "rede social/analysis_sorted/")')
    ap.add_argument("--pattern", default=None)
    ap.add_argument("--suffix", default="_clean_sorted")
    ap.add_argument("--sep", default=",", help="Separador do CSV (',' ou ';' normalmente)")
    ap.add_argument("--score-col", default="tybyria_score")
    ap.add_argument("--chunksize", type=int, default=200000)
    ap.add_argument("--dry-run", action="store_true", help="Não sobe arquivo; só calcula top10 e estatísticas básicas.")
    args = ap.parse_args()

    targets = list(list_target_blobs(args.bucket, args.prefix, args.pattern))
    if not targets:
        print("⚠️  Nenhum arquivo alvo encontrado.")
        return

    print(f"🔎 Encontrados {len(targets)} arquivo(s) alvo:")
    for t in targets:
        print(f" - gs://{args.bucket}/{t}")

    for in_blob_name in targets:
        out_blob_name = build_out_name(args.out_prefix, in_blob_name, args.suffix)

        with tempfile.TemporaryDirectory(prefix="gcs_tybyria_") as td:
            in_path = os.path.join(td, "in.csv")
            out_path = os.path.join(td, "out.csv")

            print(f"\n⬇️  Baixando: gs://{args.bucket}/{in_blob_name}")
            download_blob_to_file(args.bucket, in_blob_name, in_path)

            # detecta colunas p/ achar text col
            head = pd.read_csv(in_path, sep=args.sep, nrows=1, engine="python", on_bad_lines="skip")
            cols = list(head.columns)
            text_col = find_text_col(cols)
            if text_col is None:
                print("⚠️  Não achei coluna text_original/text_origina. Vou ordenar, mas não consigo printar os comentários.")

            print(f"🧹 Ordenando DESC por: {args.score_col}")
            top10 = external_sort_by_score(
                in_csv_path=in_path,
                out_csv_path=out_path,
                sep=args.sep,
                score_col=args.score_col,
                text_col=text_col,
                chunksize=args.chunksize,
            )

            if top10:
                print("\n🏆 TOP 10 comentários (maior score):")
                for i, (score, txt) in enumerate(top10, 1):
                    txt_clean = (txt or "").replace("\n", " ").strip()
                    if len(txt_clean) > 240:
                        txt_clean = txt_clean[:240] + "…"
                    print(f"{i:02d}) score={score:.6f} | {txt_clean}")

            if args.dry_run:
                print("\n🧪 Dry-run ativo: não vou subir o arquivo ordenado pro GCS.")
                continue

            print(f"\n⬆️  Subindo limpo+ordenado para: gs://{args.bucket}/{out_blob_name}")
            upload_file_to_blob(args.bucket, out_blob_name, out_path)
            print("✅ Pronto!")

if __name__ == "__main__":
    main()
