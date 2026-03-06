import csv
import io
import os
import sys
import tempfile
from typing import List, Set

from google.cloud import storage


def list_csv_blobs(client, bucket_name, prefix):
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    return [
        b for b in blobs
        if b.name.lower().endswith(".csv")
        and "VADERZAO" not in b.name
    ]


def linha_valida(row: dict) -> bool:
    """
    Remove:
    - linhas totalmente vazias
    - linhas com apenas espaços
    - linhas onde todos os campos são vazios
    """
    if not row:
        return False

    for value in row.values():
        if value and str(value).strip() != "":
            return True

    return False


def merge_csvs(bucket_name, prefix, output_path):
    client = storage.Client()
    blobs = list_csv_blobs(client, bucket_name, prefix)

    if not blobs:
        print("❌ Nenhum CSV encontrado.")
        sys.exit(1)

    print(f"📄 Encontrados {len(blobs)} arquivos.")

    all_columns = []
    seen: Set[str] = set()

    # Descobrir todas as colunas
    for blob in blobs:
        with blob.open("rb") as f:
            header_line = f.readline().decode("utf-8", errors="replace")
            header = next(csv.reader([header_line]))
            for col in header:
                if col not in seen:
                    seen.add(col)
                    all_columns.append(col)

    if "source_file" not in all_columns:
        all_columns.append("source_file")

    tmp_dir = tempfile.mkdtemp()
    local_file = os.path.join(tmp_dir, "VADERZAO.csv")

    total_rows = 0
    linhas_removidas = 0

    with open(local_file, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=all_columns)
        writer.writeheader()

        for blob in blobs:
            print(f"🔄 Processando {blob.name}")
            with blob.open("rb") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                )

                for row in reader:
                    if not linha_valida(row):
                        linhas_removidas += 1
                        continue

                    row["source_file"] = blob.name
                    writer.writerow(row)
                    total_rows += 1

    print(f"✅ Total de linhas válidas: {total_rows}")
    print(f"🧹 Linhas em branco removidas: {linhas_removidas}")
    print("⬆️ Subindo para GCS...")

    client.bucket(bucket_name).blob(output_path).upload_from_filename(local_file)

    print("🎉 VADERZAO criado com sucesso!")


if __name__ == "__main__":
    bucket = os.getenv("TYBYRIA_BUCKET")
    prefix = os.getenv("VADER_PREFIX", "analysis/vader/")
    output = prefix.rstrip("/") + "/VADERZAO.csv"

    if not bucket:
        print("❌ Defina a variável TYBYRIA_BUCKET")
        sys.exit(1)

    merge_csvs(bucket, prefix, output)
