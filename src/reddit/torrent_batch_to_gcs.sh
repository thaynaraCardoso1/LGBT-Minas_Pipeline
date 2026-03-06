#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Uso:"
  echo "  ./torrent_batch_to_gcs.sh <manifest_file> <gcs_dest>"
  echo
  echo "Manifest (1 por linha):"
  echo "  torrent_url|wanted_path"
  echo
  echo "Exemplo:"
  echo "  ./torrent_batch_to_gcs.sh downloads_manifest.txt \"gs://lgbtminas-dados/rede social/raw/\""
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

MANIFEST_FILE="$1"
GCS_DEST="$2"

SINGLE_SCRIPT="$HOME/LGBT-Minas_Pipeline/src/reddit/torrent_to_gcs.sh"
BASE_WORKDIR="${WORKDIR_BASE:-$HOME/torrent_batch_jobs}"

if [[ ! -f "$MANIFEST_FILE" ]]; then
  echo "❌ Manifest não encontrado: $MANIFEST_FILE"
  exit 1
fi

if [[ ! -x "$SINGLE_SCRIPT" ]]; then
  echo "❌ Script unitário não encontrado ou sem permissão de execução:"
  echo "   $SINGLE_SCRIPT"
  exit 1
fi

mkdir -p "$BASE_WORKDIR"

echo "======================================"
echo "🚀 Batch (sequencial) -> GCS"
echo "Manifest: $MANIFEST_FILE"
echo "Destino:  $GCS_DEST"
echo "Workbase: $BASE_WORKDIR"
echo "Unitário: $SINGLE_SCRIPT"
echo "======================================"

TOTAL=0
SUCCESS=0
FAIL=0

while IFS='|' read -r TORRENT_URL WANTED_PATH; do
  # ignora linhas vazias e comentários
  [[ -z "${TORRENT_URL// }" ]] && continue
  [[ "${TORRENT_URL:0:1}" == "#" ]] && continue

  TOTAL=$((TOTAL + 1))
  ITEM_NAME="$(basename "$WANTED_PATH")"
  ITEM_WORKDIR="${BASE_WORKDIR}/job_${TOTAL}_$(date +%s)"

  echo
  echo "--------------------------------------"
  echo "▶️ Item $TOTAL"
  echo "Torrent: $TORRENT_URL"
  echo "Wanted:  $WANTED_PATH"
  echo "Workdir: $ITEM_WORKDIR"
  echo "--------------------------------------"

  mkdir -p "$ITEM_WORKDIR"

  if WORKDIR_OVERRIDE="$ITEM_WORKDIR" "$SINGLE_SCRIPT" "$TORRENT_URL" "$WANTED_PATH" "$GCS_DEST"; then
    echo "✅ Item $TOTAL concluído: $ITEM_NAME"
    SUCCESS=$((SUCCESS + 1))
  else
    echo "❌ Item $TOTAL falhou: $ITEM_NAME"
    FAIL=$((FAIL + 1))
  fi

  echo "🧹 Limpando workdir residual..."
  rm -rf "$ITEM_WORKDIR" || true

  echo "💽 Espaço em disco após item $TOTAL:"
  df -h /
done < "$MANIFEST_FILE"

echo
echo "======================================"
echo "🏁 Fim do batch"
echo "Total:    $TOTAL"
echo "Sucesso:  $SUCCESS"
echo "Falhas:   $FAIL"
echo "======================================"

if [[ "$FAIL" -gt 0 ]]; then
  exit 2
fi
