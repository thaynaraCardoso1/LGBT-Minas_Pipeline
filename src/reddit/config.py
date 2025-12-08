import os

# Diretório raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Diretórios de dados
BASES_DIR = os.path.join(BASE_DIR, "bases", "rede social", "reddit")
RAW_DIR = os.path.join(BASES_DIR, "raw")
PROCESSED_DIR = os.path.join(BASES_DIR, "processed")

# Diretório correto dos filtros
FILTROS_DIR = os.path.join(BASE_DIR, "configs", "filtros")

TERMOS_LGBT_PATH = os.path.join(FILTROS_DIR, "termos_lgbt.txt")
TERMOS_ODIO_PATH = os.path.join(FILTROS_DIR, "termos_odio.txt")
CIDADES_MG_PATH = os.path.join(FILTROS_DIR, "cidades_mg.txt")

def carregar_lista(caminho_arquivo):
    termos = []
    with open(caminho_arquivo, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if linha:
                termos.append(linha.lower())
    return termos

def carregar_config_reddit():
    return {
        "termos_lgbt": carregar_lista(TERMOS_LGBT_PATH),
        "termos_odio": carregar_lista(TERMOS_ODIO_PATH),
        "cidades_mg": carregar_lista(CIDADES_MG_PATH),
    }
