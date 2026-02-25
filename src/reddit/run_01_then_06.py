import os
import sys
import traceback
import logging

from src.utils.logger import setup_logger

# Importa a função principal do seu processador de 1 arquivo
# ⚠️ Ajuste o import se seu módulo tiver outro nome/função
from src.reddit.process_one_gcs import process_file_gcs  # <- a gente vai garantir abaixo

BUCKET = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")  # pode manter
PREFIX_RAW = os.getenv("REDE_SOCIAL_RAW_PREFIX", "rede social/raw/")

FILES_TO_TRY = ["RC_2024-01.zst", "RC_2024-02.zst", "RC_2024-03.zst"]

def main():
    logger = setup_logger("logs/run_01_then_06.log")
    for h in logger.handlers:
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.info("==== RUNNER: tentar 01 e, se falhar, rodar 06 ====")

    for fname in FILES_TO_TRY:
        logger.info(f"➡️ Tentando processar: {fname}")

        try:
            ok = process_file_gcs(
                bucket_name=BUCKET,
                raw_blob_path=f"{PREFIX_RAW}{fname}",
                out_prefix_processed="rede social/processed/",
                checkpoint_prefix="rede social/tmp/",
                logger=logger,
            )
            if ok:
                logger.info(f"✅ Sucesso em {fname}. Parando runner.")
                return 0
            else:
                logger.warning(f"⚠️ {fname} retornou ok=False. Tentando próximo.")
        except Exception as e:
            logger.error(f"❌ Falhou em {fname}: {e}")
            logger.error(traceback.format_exc())
            logger.info("➡️ Indo para o próximo arquivo...")

    logger.error("🚨 Nenhum arquivo processou com sucesso (01 e 06 falharam).")
    return 1

if __name__ == "__main__":
    sys.exit(main())
