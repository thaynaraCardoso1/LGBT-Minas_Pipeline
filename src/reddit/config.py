import os

# Diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIGS_DIR = os.path.join(BASE_DIR, "configs", "filtros")

# Diretórios de dados
BASES_DIR = os.path.join(BASE_DIR, "bases", "rede social", "reddit")
RAW_DIR = os.path.join(BASES_DIR, "raw")
PROCESSED_DIR = os.path.join(BASES_DIR, "processed")

# Diretório correto dos filtros
FILTROS_DIR = os.path.join(BASE_DIR, "configs", "filtros")

TERMOS_LGBT_PATH = os.path.join(FILTROS_DIR, "termos_lgbt.txt")
TERMOS_ODIO_PATH = os.path.join(FILTROS_DIR, "termos_odio.txt")
CIDADES_MG_PATH = os.path.join(FILTROS_DIR, "cidades_mg.txt")
SUBREDDITS_BR_PATH = os.path.join(CONFIGS_DIR, "subreddits_br.txt")


def carregar_lista(caminho_arquivo):
    termos = []
    with open(caminho_arquivo, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if linha:
                termos.append(linha.lower())
    return termos

def carregar_config_reddit():
    termos_lgbt   = carregar_lista(TERMOS_LGBT_PATH)
    termos_odio   = carregar_lista(TERMOS_ODIO_PATH)
    cidades_mg    = carregar_lista(CIDADES_MG_PATH)
    subreddits_br = carregar_lista(SUBREDDITS_BR_PATH)

    return {
        "termos_lgbt": termos_lgbt,
        "termos_odio": termos_odio,
        "cidades_mg": cidades_mg,
        "subreddits_br": subreddits_br,
    }

