import re
import unicodedata

def limpar_texto(texto: str) -> str:
    if not isinstance(texto, str):
        return ""

    # Remove URLs
    texto = re.sub(r"http\S+|www\S+|https\S+", "", texto)

    # Remove emojis e símbolos
    texto = "".join(
        c for c in texto
        if unicodedata.category(c)[0] != "So"
    )

    # Remove pontuação
    texto = re.sub(r"[^\w\s]", " ", texto)

    # Remove números
    texto = re.sub(r"\d+", " ", texto)

    # Lowercase
    texto = texto.lower()

    # Remove espaços duplicados
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto
