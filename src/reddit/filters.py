import re
from typing import List, Tuple


def tokenize(texto: str) -> List[str]:
    """Tokeniza o texto em palavras alfanuméricas."""
    if not isinstance(texto, str):
        return []
    return re.findall(r"\w+", texto.lower(), flags=re.UNICODE)


def separar_simples_composto(termos: List[str]):
    """Separa termos de 1 palavra (usar regex) e termos compostos (usar substring)."""
    simples = []
    compostos = []
    for t in termos:
        t = t.strip().lower()
        if not t:
            continue
        if " " in t:
            compostos.append(t)
        else:
            simples.append(t)
    return simples, compostos


def match_simples(texto_lower: str, tokens: List[str], termos: List[str]) -> List[str]:
    """Match de termos de 1 palavra usando regex com borda de palavra."""
    encontrados = []
    for term in termos:
        pattern = rf"\b{re.escape(term)}\b"
        if re.search(pattern, texto_lower):
            encontrados.append(term)
    return encontrados


def match_compostos(texto_lower: str, termos: List[str]) -> List[str]:
    """Match de termos compostos usando substring normal."""
    return [term for term in termos if term in texto_lower]


def texto_casa_mg_lgbt(
    texto: str,
    termos_lgbt: List[str],
    termos_odio: List[str],
    cidades_mg: List[str],
) -> Tuple[bool, List[str], List[str]]:
    """
    Retorna:
      ok → match válido
      matched_termos → termos LGBT/ódio encontrados
      matched_cidades → cidades MG encontradas
    """
    if not isinstance(texto, str):
        return False, [], []

    texto_lower = texto.lower()

    # Tokenização (não usada diretamente, mas pode ser útil)
    tokens = tokenize(texto)

    # --- Separar simples e compostos ---
    lgbt_s, lgbt_c = separar_simples_composto(termos_lgbt)
    odio_s, odio_c = separar_simples_composto(termos_odio)
    cid_s, cid_c   = separar_simples_composto(cidades_mg)

    # --- Match LGBT e ódio ---
    matched_lgbt   = match_simples(texto_lower, tokens, lgbt_s) + match_compostos(texto_lower, lgbt_c)
    matched_odio   = match_simples(texto_lower, tokens, odio_s) + match_compostos(texto_lower, odio_c)
    matched_termos = matched_lgbt + matched_odio

    # --- Match cidades MG ---
    matched_cidades = match_simples(texto_lower, tokens, cid_s) + match_compostos(texto_lower, cid_c)

    # --- Lógica final ---
    ok = (len(matched_termos) > 0) and (len(matched_cidades) > 0)

    return ok, matched_termos, matched_cidades
