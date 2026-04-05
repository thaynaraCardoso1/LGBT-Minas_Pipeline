#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Uso:"
  echo "  ./torrent_to_gcs.sh <torrent_url_ou_magnet> <wanted_path> <gcs_dest>"
  echo ""
  echo "Exemplo:"
  echo "  ./torrent_to_gcs.sh \\"
  echo "    https://academictorrents.com/download/481bf2eac43172ae724fd6c75dbcb8e27de77734.torrent \\"
  echo "    comments/RC_2025-12.zst \\"
  echo "    \"gs://lgbtminas-dados/rede social/raw/\""
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 3 ]]; then
  usage
  exit 1
fi

TORRENT_SOURCE="$1"
WANTED_PATH="$2"
GCS_DEST="$3"

WORKDIR="${WORKDIR_OVERRIDE:-$HOME/torrent_job_$(date +%s)}"

echo "======================================"
echo "🚀 Torrent → GCS (arquivo específico)"
echo "======================================"
echo "Torrent:  $TORRENT_SOURCE"
echo "Wanted:   $WANTED_PATH"
echo "Destino:  $GCS_DEST"
echo "Workdir:  $WORKDIR"
echo "======================================"

mkdir -p "$WORKDIR"
cd "$WORKDIR"

if ! command -v aria2c >/dev/null 2>&1; then
  echo "📦 Instalando aria2..."
  sudo apt update -y
  sudo apt install -y aria2
fi

if ! command -v gsutil >/dev/null 2>&1; then
  echo "❌ gsutil não encontrado. Instale o Google Cloud SDK / configure a VM com acesso ao GCS."
  exit 1
fi

TORRENT_FILE=""
if [[ "$TORRENT_SOURCE" == http*".torrent" ]]; then
  echo "⬇️ Baixando .torrent..."
  curl -L --fail -o file.torrent "$TORRENT_SOURCE"
  TORRENT_FILE="file.torrent"
elif [[ "$TORRENT_SOURCE" == magnet:* ]]; then
  TORRENT_FILE="$TORRENT_SOURCE"
else
  TORRENT_FILE="$TORRENT_SOURCE"
fi

echo "🔎 Procurando o arquivo dentro do torrent..."

TORRENT_LISTING="$(aria2c -S "$TORRENT_FILE" 2>/dev/null || true)"

IDX="$(printf '%s\n' "$TORRENT_LISTING" \
  | grep -F "$WANTED_PATH" \
  | head -n 1 \
  | cut -d'|' -f1 \
  | tr -d '[:space:]')"

if [[ -z "${IDX}" ]]; then
  echo "❌ Não achei '$WANTED_PATH' na lista do torrent."
  echo "👉 Algumas linhas da listagem:"
  printf '%s\n' "$TORRENT_LISTING" | head -n 40
  echo "👉 Linhas parecidas:"
  printf '%s\n' "$TORRENT_LISTING" | grep -E 'reddit/comments|comments/RC_|filecomments/RC_|RC_' | head -n 30 || true
  exit 1
fi

echo "✅ Índice encontrado: $IDX"
echo "📥 Baixando SOMENTE o arquivo desejado..."
echo "   (sem pré-alocação gigante; sem seed depois)"

aria2c \
  --select-file="$IDX" \
  --dir="$WORKDIR" \
  --seed-time=0 \
  --file-allocation=none \
  "$TORRENT_FILE"

echo "🔎 Localizando arquivo baixado..."
LOCAL_FILE="$(find "$WORKDIR" -type f -name "$(basename "$WANTED_PATH")" | head -n 1 || true)"

if [[ -z "${LOCAL_FILE}" ]]; then
  echo "❌ Download terminou, mas não encontrei '$(basename "$WANTED_PATH")' em $WORKDIR"
  echo "📂 Arquivos encontrados:"
  find "$WORKDIR" -maxdepth 6 -type f -print
  exit 1
fi

echo "☁️ Enviando para o GCS..."
echo "   $LOCAL_FILE -> $GCS_DEST"

if gsutil -m cp "$LOCAL_FILE" "$GCS_DEST"; then
  echo "✅ Upload ok."
  echo "🧹 Limpando arquivo local..."
  rm -f "$LOCAL_FILE"
  find "$WORKDIR" -type f -name "*.aria2" -delete || true
  find "$WORKDIR" -type d -empty -delete || true
  rmdir "$WORKDIR" 2>/dev/null || true
else
  echo "❌ Falha no upload. Arquivo mantido localmente:"
  echo "   $LOCAL_FILE"
  exit 2
fi

echo "======================================"
echo "🎉 Concluído!"
echo "Destino: $GCS_DEST"
echo "======================================"
