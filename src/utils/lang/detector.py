from langdetect import detect, detect_langs, DetectorFactory

# para resultados reproduzíveis
DetectorFactory.seed = 0


def is_portuguese(texto: str, threshold: float = 0.80) -> bool:
    """
    Retorna True se o texto for detectado como português (pt)
    com confiança >= threshold.
    """
    if not isinstance(texto, str) or not texto.strip():
        return False

    try:
        langs = detect_langs(texto)
        # langs é algo como: [pt:0.92, es:0.05, ...]
        best = langs[0]
        lang = best.lang
        prob = best.prob
        return lang == "pt" and prob >= threshold
    except Exception:
        return False


def get_lang(texto: str):
    """
    Retorna (lang, prob) para debug.
    """
    if not isinstance(texto, str) or not texto.strip():
        return "none", 0.0

    try:
        langs = detect_langs(texto)
        best = langs[0]
        return best.lang, best.prob
    except Exception:
        return "err", 0.0
