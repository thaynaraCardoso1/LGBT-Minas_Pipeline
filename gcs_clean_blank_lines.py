#!/usr/bin/env python3
import argparse
import re
import sys
from google.cloud import storage

BLANK_RE = re.compile(r'^[\s,;"]*$')  # linha só com whitespace e/ou separadores comuns

def is_blank_line(line: str) -> bool:
    return BLANK_RE.match(line) is not None

def ensure_trailing_slash(p: str) -> str:
    return p if p.endswith("/") else p + "/"

def build_out_name(out_prefix: str, in_blob_name: str, suffix: str) -> str:
    # mantém só o nome do arquivo (sem pastas) pra salvar no out_prefix
    filename = in_blob_name.split("/")[-1]
    if filename.lower().endswith(".csv"):
        base = filename[:-4]
        return f"{ensure_trailing_slash(out_prefix)}{base}{suffix}.csv"
    return f"{ensure_trailing_slash(out_prefix)}{filename}{suffix}"

def clean_blob_to_new(bucket_name: str, in_blob_name: str, out_blob_name: str, dry_run: bool) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    in_blob = bucket.blob(in_blob_name)

    if not in_blob.exists(client):
        print(f"❌ Não achei: gs://{bucket_name}/{in_blob_name}", file=sys.stderr)
        return

    print(f"\n📄 Lendo:  gs://{bucket_name}/{in_blob_name}")
    print(f"🧼 Saindo: gs://{bucket_name}/{out_blob_name}")

    removed = 0
    kept = 0

    if dry_run:
        with in_blob.open("rt", encoding="utf-8", errors="replace") as r:
            for line in r:
                if is_blank_line(line):
                    removed += 1
                else:
                    kept += 1
        print(f"🧪 Dry-run: manteria {kept} linhas, removeria {removed} linhas em branco.")
        return

    out_blob = bucket.blob(out_blob_name)

    with in_blob.open("rt", encoding="utf-8", errors="replace") as r, \
         out_blob.open("wt", encoding="utf-8", newline="\n") as w:
        for line in r:
            if is_blank_line(line):
                removed += 1
                continue
            w.write(line)
            kept += 1

    print(f"✅ Concluído! Mantidas: {kept} | Removidas (em branco): {removed}")

def list_target_blobs(bucket_name: str, prefix: str, pattern: str | None):
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

def main():
    ap = argparse.ArgumentParser(description="Remove linhas em branco de CSVs no GCS e grava uma versão limpa separada.")
    ap.add_argument("--bucket", required=True, help="Nome do bucket (ex: lgbtminas-dados)")
    ap.add_argument("--prefix", required=True, help='Prefixo/pasta de ENTRADA (ex: "rede social/analysis/")')
    ap.add_argument("--out-prefix", required=True, help='Prefixo/pasta de SAÍDA (ex: "rede social/analysis_clean/")')
    ap.add_argument("--pattern", default=None,
                    help="Filtro de arquivos. Pode ser REGEX (ex: RC_2025-02.*\\.csv) ou substring.")
    ap.add_argument("--suffix", default="_clean", help='Sufixo do arquivo de saída (default: "_clean")')
    ap.add_argument("--dry-run", action="store_true", help="Não grava nada; só conta.")
    args = ap.parse_args()

    targets = list(list_target_blobs(args.bucket, args.prefix, args.pattern))
    if not targets:
        print("⚠️  Nenhum arquivo alvo encontrado com esses filtros.")
        return

    print(f"🔎 Encontrados {len(targets)} arquivo(s) alvo:")
    for t in targets:
        print(f" - gs://{args.bucket}/{t}")

    for in_name in targets:
        out_name = build_out_name(args.out_prefix, in_name, args.suffix)
        clean_blob_to_new(args.bucket, in_name, out_name, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
