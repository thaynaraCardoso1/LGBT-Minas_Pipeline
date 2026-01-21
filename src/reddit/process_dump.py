import csv
import os
import json
import time
import logging
import glob
import zstandard as zstd

from .config import RAW_DIR, PROCESSED_DIR, carregar_config_reddit
from src.utils.logger import setup_logger
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto

def extract_text(obj):
    if "body" in obj:
        return obj.get("body", "")
    return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")

def iter_zst(filepath, skip_to=0, logger=None, filename=""):
    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as f:
        with dctx.stream_reader(f) as reader:
            buffer = ""
            total_lidas = 0
            while True:
                chunk = reader.read(2**20)
                if not chunk: break
                text = chunk.decode("utf-8", errors="ignore")
                buffer += text
                linhas = buffer.split("\n")
                buffer = linhas[-1]
                for linha in linhas[:-1]:
                    total_lidas += 1
                    if total_lidas <= skip_to:
                        if total_lidas % 2000000 == 0:
                            logger.info(f"[{filename}] ⏭️ Pulando: {total_lidas:,} linhas...")
                        continue
                    if logger and total_lidas % 1000000 == 0:
                        logger.info(f"[{filename}] 📖 Lendo: {total_lidas:,} linhas...")
                    linha = linha.strip()
                    if not linha: continue
                    try:
                        yield json.loads(linha), total_lidas
                    except:
                        continue

def main():
    logger = setup_logger("logs/reddit_processamento.log")
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    logger.info("==== INÍCIO DA ESTEIRA AUTOMATIZADA COM CHECKPOINT ====")

    cfg = carregar_config_reddit()
    subreddits_br = cfg["subreddits_br"]
    
    # Busca todos os arquivos .zst
    arquivos_zst = sorted(glob.glob(os.path.join(RAW_DIR, "*.zst")))
    
    campos = ["id", "author", "created_utc", "subreddit", "text_original", "text_clean", "has_lgbt_term", "has_hate_term", "has_mg_city"]

    for caminho_zst in arquivos_zst:
        nome_f = os.path.basename(caminho_zst)
        caminho_csv = os.path.join(PROCESSED_DIR, nome_f.replace(".zst", "_BR.csv"))
        checkpoint_path = os.path.join(RAW_DIR, nome_f.replace(".zst", "_checkpoint.txt"))
        
        # Determina ponto de partida
        skip_to = 0
        modo = 'w'
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, "r") as f:
                skip_to = int(f.read().strip())
            modo = 'a'
            logger.info(f"[{nome_f}] ⚠️ Retomando da linha {skip_to:,}")
        else:
            logger.info(f"[{nome_f}] 🆕 Iniciando novo arquivo")

        encontrados = 0
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        with open(caminho_csv, modo, newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=campos)
            if modo == 'w': writer.writeheader()

            for obj, num_linha in iter_zst(caminho_zst, skip_to, logger, nome_f):
                subreddit = (obj.get("subreddit") or "").lower()

                if subreddit in subreddits_br:
                    texto_original = extract_text(obj)
                    texto_limpo = limpar_texto(texto_original)
                    _, m_termos, m_cidades = texto_casa_mg_lgbt(texto_limpo, cfg["termos_lgbt"], cfg["termos_odio"], cfg["cidades_mg"])

                    encontrados += 1
                    writer.writerow({
                        "id": obj.get("id"), "author": obj.get("author"), "created_utc": obj.get("created_utc"),
                        "subreddit": obj.get("subreddit"), "text_original": texto_original, "text_clean": texto_limpo,
                        "has_lgbt_term": int(any(t in m_termos for t in cfg["termos_lgbt"])),
                        "has_hate_term": int(any(t in m_termos for t in cfg["termos_odio"])),
                        "has_mg_city": int(bool(m_cidades)),
                    })

                # Salva checkpoint a cada 100k linhas
                if num_linha % 100000 == 0:
                    with open(checkpoint_path, "w") as f_cp:
                        f_cp.write(str(num_linha))

        # Se terminou o arquivo sem erros, remove o checkpoint
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
        logger.info(f"[{nome_f}] ✅ Concluído! Total BR: {encontrados}")

    logger.info("🏁 ESTEIRA FINALIZADA!")

if __name__ == "__main__":
    main()
    