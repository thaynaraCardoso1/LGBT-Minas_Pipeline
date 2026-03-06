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
    # remove BOM
    if header and header[0].startswith("\ufeff"):
        header[0] = header[0].lstrip("\ufeff")
    # strip
    header = [(c or "").strip() for c in header]
    # remove vazios
    header = [c for c in header if c]
    return header


def _value_to_str(v: Any) -> str:
    """
    Converte valor para string limpinha.
    - list/tuple -> junta
    - None -> ""
    - outros -> str().strip()
    """
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        # às vezes vem row[None] = ['extra1','extra2'] ou ['']
        joined = " ".join([str(x) for x in v if x is not None])
        return joined.strip()
    return str(v).strip()


def normalize_row(row: Dict[Any, Any]) -> Dict[str, str]:
    """
    Remove chave None (extras) e normaliza valores.
    """
    clean: Dict[str, str] = {}

    for k, v in row.items():
        if k is None:
            # ignora "extras" (quando tem mais colunas do que o header)
            continue
        key = str(k).strip()
        if not key:
            continue
        clean[key] = _value_to_str(v)

    return clean


def has_real_content(row: Dict[str, str], source_col: str = SOURCE_COL) -> bool:
    """
    Decide se a linha tem conteúdo real.
    Regra: existe pelo menos 1 coluna (tirando source_col) com texto != "".
    """
    for k, v in row.items():
        if k == source_col:
            continue
        if v is not None and str(v).strip() != "":
            return True
    return False


def merge_vader_csvs(bucket_name: str, prefix: str, output_blob_path: str, source_col: str = SOURCE_COL) -> None:
    client = storage.Client()
    blobs = list_csv_blobs(client, bucket_name, prefix)

    if not blobs:
        print(f"❌ Nenhum CSV encontrado em gs://{bucket_name}/{prefix}")
        sys.exit(1)

    print(f"📄 Encontrados {len(blobs)} CSVs em gs://{bucket_name}/{prefix}")
    for b in blobs[:8]:
        print(f"  - {b.name}")
    if len(blobs) > 8:
        print(f"  ... (+{len(blobs) - 8} arquivos)")

    # União de colunas
    all_columns: List[str] = []
    seen: Set[str] = set()

    for blob in blobs:
        header = read_header_from_blob(blob)
        if not header:
            print(f"⚠️  Sem header (ou vazio): {blob.name} (pulando)")
            continue
        for col in header:
            if col not in seen:
                seen.add(col)
                all_columns.append(col)

    if not all_columns:
        print("❌ Não consegui detectar colunas. Confere se os CSVs têm header.")
        sys.exit(1)

    if source_col not in seen:
        all_columns.append(source_col)

    print(f"🧾 Colunas no VADERZAO: {len(all_columns)} (inclui '{source_col}')")

    # Saída local
    tmp_dir = tempfile.mkdtemp(prefix="vaderzao_")
    local_out = os.path.join(tmp_dir, "VADERZAO.csv")

    total_written = 0
    removed_blank = 0
    processed_files = 0

    with open(local_out, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()

        for i, blob in enumerate(blobs, start=1):
            print(f"🔄 [{i}/{len(blobs)}] Lendo {blob.name} ...")

            file_written = 0
            file_removed = 0

            with blob.open("rb") as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
                reader = csv.DictReader(text)

                if not reader.fieldnames:
                    print(f"⚠️  CSV vazio/sem header detectável: {blob.name} (pulando)")
                    continue

                for raw in reader:
                    # normaliza (remove None/extras + strip)
                    row = normalize_row(raw)

                    # Se não tiver conteúdo real nas colunas do CSV, descarta
                    if not has_real_content(row, source_col=source_col):
                        removed_blank += 1
                        file_removed += 1
                        continue

                    row[source_col] = blob.name
                    writer.writerow(row)

                    total_written += 1
                    file_written += 1

            processed_files += 1
            print(f"   ✅ {blob.name}: escritas={file_written}, removidas(vazias)={file_removed}")

    print(f"\n✅ Arquivos processados: {processed_files}/{len(blobs)}")
    print(f"✅ Total de linhas válidas no VADERZAO: {total_written}")
    print(f"🧹 Linhas vazias removidas: {removed_blank}")
    print(f"⬆️  Subindo para gs://{bucket_name}/{output_blob_path}")

    out_blob = client.bucket(bucket_name).blob(output_blob_path)
    out_blob.upload_from_filename(local_out, content_type="text/csv")

    print("🎉 Pronto! VADERZAO gerado sem linhas em branco.")


if __name__ == "__main__":
    bucket = (os.getenv("TYBYRIA_BUCKET") or "").strip()
    prefix = (os.getenv("VADER_PREFIX") or "analysis/vader/").strip()
    output = (os.getenv("VADER_OUTPUT") or "").strip()

    if not bucket:
        print("❌ Defina TYBYRIA_BUCKET. Ex: export TYBYRIA_BUCKET='lgbtminas-dados'")
        sys.exit(1)

    if not output:
        p = prefix.rstrip("/") + "/"
        output = f"{p}VADERZAO.csv"

    merge_vader_csvs(bucket_name=bucket, prefix=prefix, output_blob_path=output)
