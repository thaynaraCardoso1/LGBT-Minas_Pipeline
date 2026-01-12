import csv
import os
import json
import time
import logging  # 👈 Importado para garantir o formato do log
import zstandard as zstd

from .config import RAW_DIR, PROCESSED_DIR, carregar_config_reddit
from src.utils.logger import setup_logger
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto

# Configurações de arquivo
ARQUIVO_ZST = "RC_2025-03.zst"
CSV_SAIDA   = ARQUIVO_ZST.replace(".zst", "_BR.csv")

def extract_text(obj):
    """Extrai o texto do post (seja comentário ou submissão)."""
    if "body" in obj:
        return obj.get("body", "")
    else:
        return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")

def iter_zst(filepath, logger=None):
    """Lê o arquivo comprimido .zst linha por linha."""
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as f:
        with dctx.stream_reader(f) as reader:
            buffer = ""
            total = 0

            while True:
                chunk = reader.read(2**20) # 1MB chunks
                if not chunk:
                    break

                text = chunk.decode("utf-8", errors="ignore")
                buffer += text
                linhas = buffer.split("\n")
                buffer = linhas[-1]

                for linha in linhas[:-1]:
                    total += 1
                    # Log de progresso de leitura bruta (a cada 1 milhão de linhas)
                    if logger and total % 1000000 == 0:
                        logger.info(f"Lidas {total:,} linhas do dump bruto...")

                    linha = linha.strip()
                    if not linha:
                        continue

                    try:
                        yield json.loads(linha)
                    except:
                        continue

def main():
    # Configuração do logger com timestamp detalhado
    logger = setup_logger("logs/reddit_processamento.log")
    
    # Ajustando o formatador do logger para incluir data/hora caso o setup_logger não o faça
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    inicio = time.time()

    logger.info("==== INÍCIO DO PROCESSAMENTO (APENAS SUBREDDITS BR) ====")

    # Carrega configurações e listas
    cfg = carregar_config_reddit()
    termos_lgbt   = cfg["termos_lgbt"]
    termos_odio   = cfg["termos_odio"]
    cidades_mg    = cfg["cidades_mg"]
    subreddits_br = cfg["subreddits_br"]

    caminho_zst = os.path.join(RAW_DIR, ARQUIVO_ZST)
    caminho_csv = os.path.join(PROCESSED_DIR, CSV_SAIDA)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    logger.info(f"📂 Arquivo de origem: {caminho_zst}")
    logger.info(f"💾 Arquivo de destino: {caminho_csv}")

    campos = [
        "id", "author", "created_utc", "subreddit",
        "text_original", "text_clean",
        "has_lgbt_term", "has_hate_term", "has_mg_city",
    ]

    encontrados = 0

    with open(caminho_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        writer.writeheader()

        for obj in iter_zst(caminho_zst, logger):
            subreddit = (obj.get("subreddit") or "").lower()

            # FILTRO ÚNICO: Subreddits BR
            if subreddit not in subreddits_br:
                continue

            texto_original = extract_text(obj)
            texto_limpo = limpar_texto(texto_original)

            # Analisamos os termos apenas para marcação nas colunas
            _, matched_termos, matched_cidades = texto_casa_mg_lgbt(
                texto_limpo,
                termos_lgbt,
                termos_odio,
                cidades_mg
            )

            encontrados += 1

            writer.writerow({
                "id": obj.get("id"),
                "author": obj.get("author"),
                "created_utc": obj.get("created_utc"),
                "subreddit": obj.get("subreddit"),
                "text_original": texto_original,
                "text_clean": texto_limpo,
                "has_lgbt_term": int(any(t in matched_termos for t in termos_lgbt)),
                "has_hate_term": int(any(t in matched_termos for t in termos_odio)),
                "has_mg_city": int(bool(matched_cidades)),
            })

            # Log a cada 10 mil posts brasileiros encontrados
            if encontrados % 10000 == 0:
                logger.info(f"Status: {encontrados} posts BR já extraídos para o CSV...")

    fim = time.time()
    logger.info("🎉 Processamento finalizado com sucesso!")
    logger.info(f"📊 Total de registros salvos: {encontrados}")
    logger.info(f"⏱ Tempo total de execução: {fim - inicio:.2f} segundos")
    logger.info("==== FIM ====")

if __name__ == "__main__":
    main()