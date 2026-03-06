import csv
import io
import os
import sys
import tempfile
from typing import List, Set, Dict, Any

from google.cloud import storage

SOURCE_COL = "source_file"


def list_csv_blobs(client: storage.Client, bucket_name: str, prefix: str) -> List[storage.Blob]:
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))

    out = []
    for b in blobs:
        name = b.name
        if name.endswith("/"):
            continue
        if not name.lower().endswith(".csv"):
            continue
        if "VADERZAO" in os.path.basename(name):
            continue
        out.append(b)
    return sorted(out, key=lambda x: x.name)


def read_header_from_blob(blob: storage.Blob) -> List[str]:
    with blob.open("rb") as f:
        first = f.readline()
    if not first:
        return []
    header = next(csv.reader([first.decode("utf-8", errors="replace")]))
    if header and header[0].startswith("\ufeff"):
        header[0] = header[0].lstrip("\ufeff")
    header = [(c or "").strip() for c in header]
    header = [c for c in header if c]
    return header


def _value_to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        joined = " ".join([str(x) for x in v if x is not None])
        return joined.strip()
    return str(v).strip()


def normalize_row(row: Dict[Any, Any]) -> Dict[str, str]:
    clean: Dict[str, str] = {}
    for k, v in row.items():
        if k is None:
            continue
        key = str(k).strip()
        if not key:
            continue
        clean[key] = _value_to_str(v)
    return clean


def is_effectively_empty(row: Dict[str, str], source_col: str = SOURCE_COL) -> bool:
    """
    Remove linha se:
    - todas as colunas (exceto source_file) são vazias/espacos
    - OU se o "conteúdo total" é literalmente "0" (linha lixo muito comum)
    """
    vals = []
    for k, v in row.items():
        if k == source_col:
            continue
        s = (v or "").strip()
        vals.append(s)

    # tudo vazio
    if all(v == "" for v in vals):
        return True

    # "só 0" (ex: linha com um único token 0)
    joined = "".join(vals).strip()
    if joined == "0":
        return True

    return False


def merge(bucket_name: str, prefix: str, output_blob_path: str, source_col: str = SOURCE_COL) -> None:
    client = storage.Client()
    blobs = list_csv_blobs(client, bucket_name, prefix)

    if not blobs:
        print(f"❌ Nenhum CSV encontrado em gs://{bucket_name}/{prefix}")
        sys.exit(1)

    print(f"📄 Encontrados {len(blobs)} CSVs em gs://{bucket_name}/{prefix}")

    # união de colunas
    all_columns: List[str] = []
    seen: Set[str] = set()

    for blob in blobs:
        header = read_header_from_blob(blob)
        if not header:
            print(f"⚠️  Sem header/arquivo vazio: {blob.name} (pulando)")
            continue
        for col in header:
            if col not in seen:
                seen.add(col)
                all_columns.append(col)

    if not all_columns:
        print("❌ Não consegui detectar colunas em nenhum CSV.")
        sys.exit(1)

    if source_col not in seen:
        all_columns.append(source_col)

    tmp_dir = tempfile.mkdtemp(prefix="vaderzao_")
    local_out = os.path.join(tmp_dir, "VADERZAO.csv")

    total_written = 0
    removed = 0

    with open(local_out, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()

        for i, blob in enumerate(blobs, start=1):
            print(f"🔄 [{i}/{len(blobs)}] {blob.name}")
            with blob.open("rb") as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
                reader = csv.DictReader(text)

                if not reader.fieldnames:
                    print(f"⚠️  CSV sem header detectável: {blob.name} (pulando)")
                    continue

                for raw in reader:
                    row = normalize_row(raw)

                    if is_effectively_empty(row, source_col=source_col):
                        removed += 1
                        continue

                    row[source_col] = blob.name
                    writer.writerow(row)
                    total_written += 1

    print(f"✅ Linhas escritas: {total_written}")
    print(f"🧹 Linhas removidas (vazias/0): {removed}")
    print(f"⬆️ Subindo para gs://{bucket_name}/{output_blob_path}")

    client.bucket(bucket_name).blob(output_blob_path).upload_from_filename(local_out, content_type="text/csv")
    print("🎉 VADERZAO gerado com filtro nuclear.")


if __name__ == "__main__":
    bucket = (os.getenv("TYBYRIA_BUCKET") or "").strip()
    prefix = (os.getenv("VADER_PREFIX") or "rede social/analysis/vader/").strip()
    output = (os.getenv("VADER_OUTPUT") or "").strip()

    if not bucket:
        print("❌ Defina TYBYRIA_BUCKET (ex: export TYBYRIA_BUCKET='lgbtminas-dados')")
        sys.exit(1)

    if not output:
        p = prefix.rstrip("/") + "/"
        output = f"{p}VADERZAO.csv"

    merge(bucket_name=bucket, prefix=prefix, output_blob_path=output)
