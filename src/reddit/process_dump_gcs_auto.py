import csv
import os
import json
import logging
import subprocess
from typing import Iterator, Tuple, Optional

import zstandard as zstd

from src.utils.logger import setup_logger
from src.reddit.filters import texto_casa_mg_lgbt
from src.utils.limpeza import limpar_texto
from src.reddit.config import carregar_config_reddit

# ========= CONFIG GCS =========
BUCKET = "lgbtminas-dados"
RAW_PREFIX = "rede social/raw/"
PROCESSED_PREFIX = "rede social/processed/"
TMP_PREFIX = "rede social/tmp/"

# Pula estes arquivos (como você pediu)
SKIP_FILES = {"RC_2025-01.zst", "RC_2025-02.zst"}

# Assinatura do Zstandard (magic bytes)
ZSTD_MAGIC = bytes([0x28, 0xB5, 0x2F, 0xFD])

# Onde guardar outputs no disco da VM (persistente)
LOCAL_WORKDIR = os.path.join(os.getcwd(), "_work_reddit")
LOCAL_OUTDIR = os.path.join(LOCAL_WORKDIR, "processed")
LOCAL_CKPTDIR = os.path.join(LOCAL_WORKDIR, "checkpoints")
os.makedirs(LOCAL_OUTDIR, exist_ok=True)
os.makedirs(LOCAL_CKPTDIR, exist_ok=True)


def extract_text(obj):
    if "body" in obj:
        return obj.get("body", "")
    return (obj.get("title", "") or "") + " " + (obj.get("selftext", "") or "")


def run_cmd(cmd: list[str]) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, out, err


def gcs_list_raw_zst(logger) -> list[str]:
    # Lista URIs do tipo gs://.../RC_2025-xx.zst
    code, out, err = run_cmd(["gsutil", "ls", f"gs://{BUCKET}/{RAW_PREFIX}"])
    if code != 0:
        raise RuntimeError(f"Falha ao listar raw: {err.strip()}")
    uris = [line.strip() for line in out.splitlines() if line.strip().endswith(".zst")]
    return sorted(uris)


def gcs_read_first_bytes(uri: str, n: int = 4) -> bytes:
    # Pega os primeiros n bytes via gsutil cat | head -c n
    p1 = subprocess.Popen(["gsutil", "cat", uri], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["head", "-c", str(n)], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    data = p2.communicate()[0] or b""
    p1.wait()
    return data


def gcs_get_checkpoint(gcs_ckpt_uri: str) -> Optional[int]:
    # Tenta ler checkpoint do GCS
    code, out, _ = run_cmd(["gsutil", "cat", gcs_ckpt_uri])
    if code != 0:
        return None
    txt = (out or "").strip().replace(",", "")
    if not txt:
        return None
    try:
        return int(txt)
    except ValueError:
        return None


def gcs_put_checkpoint(gcs_ckpt_uri: str, value: int, logger):
    # Escreve checkpoint local e sobe
    local_ckpt = os.path.join(LOCAL_CKPTDIR, os.path.basename(gcs_ckpt_uri))
    with open(local_ckpt, "w", encoding="utf-8") as f:
        f.write(str(value))
    # upload silencioso
    code, _, err = run_cmd(["gsutil", "-q", "cp", local_ckpt, gcs_ckpt_uri])
    if code == 0:
        logger.info(f"☁️ Checkpoint enviado ao GCS: {gcs_ckpt_uri.replace(f'gs://{BUCKET}/','')} = {value:,}")
    else:
        logger.warning(f"⚠️ Falha ao subir checkpoint: {err.strip()}")


def gcs_remove_checkpoint(gcs_ckpt_uri: str, logger):
    # remove local e gcs (se existir)
    local_ckpt = os.path.join(LOCAL_CKPTDIR, os.path.basename(gcs_ckpt_uri))
    try:
        if os.path.exists(local_ckpt):
            os.remove(local_ckpt)
    except:
        pass
    run_cmd(["gsutil", "rm", "-f", gcs_ckpt_uri])
    logger.info("🧹 Checkpoint removido (local e GCS).")


def iter_zst_from_gsutil(uri: str, skip_to: int, logger, filename: str) -> Iterator[Tuple[dict, int]]:
    """
    Stream:
      gsutil cat gs://.../file.zst  -> stdout (bytes)
      zstd stream_reader            -> bytes decomprimidos (texto jsonl)
    """
    proc = subprocess.Popen(["gsutil", "cat", uri], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.stdout is not None

    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(proc.stdout) as reader:
        buffer = ""
        total = 0
        while True:
            chunk = reader.read(2**20)  # 1MB
            if not chunk:
                break
            text = chunk.decode("utf-8", errors="ignore")
            buffer += text
            linhas = buffer.split("\n")
            buffer = linhas[-1]

            for linha in linhas[:-1]:
                total += 1
                if total <= skip_to:
                    if total % 2_000_000 == 0:
                        logger.info(f"[{filename}] ⏭️ Pulando: {total:,} linhas...")
                    continue

                if total % 1_000_000 == 0:
                    logger.info(f"[{filename}] 📖 Lendo: {total:,} linhas...")

                linha = linha.strip()
                if not linha:
                    continue
                try:
                    yield json.loads(linha), total
                except:
                    continue

    # consume stderr to avoid zombies
    proc.stdout.close()
    _ = proc.stderr.read() if proc.stderr else b""
    proc.wait()


def process_one(uri: str, logger):
    nome_f = os.path.basename(uri)

    gcs_ckpt_uri = f"gs://{BUCKET}/{TMP_PREFIX}{nome_f.replace('.zst','')}_checkpoint.txt"
    gcs_out_uri = f"gs://{BUCKET}/{PROCESSED_PREFIX}{nome_f.replace('.zst','')}_BR.csv"

    local_csv = os.path.join(LOCAL_OUTDIR, nome_f.replace(".zst", "_BR.csv"))

    # carrega checkpoint (GCS tem prioridade)
    skip_to = gcs_get_checkpoint(gcs_ckpt_uri) or 0

    modo = "a" if (skip_to > 0 and os.path.exists(local_csv)) else "w"
    if skip_to > 0:
        logger.info(f"[{nome_f}] ⚠️ Retomando da linha {skip_to:,} (modo={modo})")
    else:
        logger.info(f"[{nome_f}] 🆕 Iniciando novo arquivo")

    cfg = carregar_config_reddit()
    subreddits_br = set(cfg["subreddits_br"])

    campos = [
        "id", "author", "created_utc", "subreddit",
        "text_original", "text_clean",
        "has_lgbt_term", "has_hate_term", "has_mg_city"
    ]

    encontrados = 0

    with open(local_csv, modo, newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=campos)
        if modo == "w":
            writer.writeheader()

        for obj, num_linha in iter_zst_from_gsutil(uri, skip_to, logger, nome_f):
            subreddit = (obj.get("subreddit") or "").lower()

            if subreddit in subreddits_br:
                texto_original = extract_text(obj)
                texto_limpo = limpar_texto(texto_original)

                _, m_termos, m_cidades = texto_casa_mg_lgbt(
                    texto_limpo,
                    cfg["termos_lgbt"],
                    cfg["termos_odio"],
                    cfg["cidades_mg"],
                )

                encontrados += 1
                writer.writerow({
                    "id": obj.get("id"),
                    "author": obj.get("author"),
                    "created_utc": obj.get("created_utc"),
                    "subreddit": obj.get("subreddit"),
                    "text_original": texto_original,
                    "text_clean": texto_limpo,
                    "has_lgbt_term": int(any(t in m_termos for t in cfg["termos_lgbt"])),
                    "has_hate_term": int(any(t in m_termos for t in cfg["termos_odio"])),
                    "has_mg_city": int(bool(m_cidades)),
                })

            # checkpoint a cada 100k linhas
            if num_linha % 100_000 == 0:
                gcs_put_checkpoint(gcs_ckpt_uri, num_linha, logger)

    logger.info(f"[{nome_f}] ✅ Processamento local concluído. Total BR: {encontrados:,}")

    # upload final via gsutil (mais simples e está funcionando no seu projeto)
    cmd = ["gsutil", "-q", "cp", local_csv, gcs_out_uri]
    logger.info(f"☁️ Upload via gsutil: {' '.join(cmd)}")
    code, _, err = run_cmd(cmd)
    if code != 0:
        raise RuntimeError(f"Falha no upload do CSV: {err.strip()}")

    gcs_remove_checkpoint(gcs_ckpt_uri, logger)
    logger.info(f"[{nome_f}] ✅ CSV no GCS: {gcs_out_uri.replace(f'gs://{BUCKET}/','')}")


def main():
    logger = setup_logger("logs/reddit_processamento_auto_gcs.log")
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    logger.info("==== INÍCIO (AUTO GCS) ====")

    uris = gcs_list_raw_zst(logger)
    logger.info(f"Arquivos .zst encontrados no GCS: {len(uris)}")

    # filtra RC_2025 e exclui 01/02
    candidatos = []
    for uri in uris:
        nome = os.path.basename(uri)
        if not nome.startswith("RC_2025-"):
            continue
        if nome in SKIP_FILES:
            logger.info(f"[{nome}] ⏭️ Ignorado (skip list).")
            continue
        candidatos.append(uri)

    if not candidatos:
        logger.info("Nada para processar (após filtros).")
        return

    logger.info(f"Arquivos elegíveis: {len(candidatos)}")

    for uri in candidatos:
        nome = os.path.basename(uri)

        # healthcheck do zst
        head4 = gcs_read_first_bytes(uri, 4)
        if head4 != ZSTD_MAGIC:
            logger.warning(f"[{nome}] ❌ Parece corrompido (magic={head4.hex(' ')}). Pulando.")
            continue

        try:
            process_one(uri, logger)
        except Exception as e:
            logger.exception(f"[{nome}] 💥 Erro no processamento: {e}")
            # não mata a esteira inteira; segue pro próximo
            continue

    logger.info("🏁 FIM (AUTO GCS).")


if __name__ == "__main__":
    main()
