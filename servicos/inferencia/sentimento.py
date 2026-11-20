"""
Classificador de sentimento por lista de palavras (offline, deterministico).

E o "modelo" servido por POST /predict. Propositalmente simples: o foco da disciplina
e a plataforma (fila, worker, resiliencia, rastreamento), nao o modelo. Por ter a
mesma interface de um modelo de verdade (texto -> sentimento + confianca), pode ser
trocado sem mexer no resto do sistema.

Melhorias sobre a versao antiga (inferencia/app.py):
  - acentos sao removidos antes de comparar: "otimo" e "ótimo", "pessimo" e "péssimo"
    contam igual (a lista e escrita sem acento);
  - tokens sao extraidos por expressao regular, entao pontuacao colada na palavra
    ("bom!", "ruim?", "(lento)") nao impede o reconhecimento.
"""
import re
import unicodedata

PALAVRAS_POSITIVAS = frozenset({
    "excelente", "otimo", "otima", "bom", "boa", "rapido", "rapida", "perfeito",
    "perfeita", "maravilhoso", "maravilhosa", "recomendo", "adorei", "gostei", "amei",
})
PALAVRAS_NEGATIVAS = frozenset({
    "ruim", "pessimo", "pessima", "lento", "lenta", "erro", "falha", "horrivel",
    "problema", "defeito", "odiei", "travou", "quebrado", "quebrou",
})


def normalizar(texto: str) -> str:
    """Minusculas e sem acentos (NFKD separa a letra do acento; o acento e descartado)."""
    decomposto = unicodedata.normalize("NFKD", texto.lower())
    return "".join(c for c in decomposto if not unicodedata.combining(c))


def tokens(texto: str) -> set:
    """Palavras distintas do texto, ja normalizadas. Pontuacao e numeros soltos sao ignorados."""
    return set(re.findall(r"[^\W\d_]+", normalizar(texto)))


def classificar(texto: str) -> dict:
    """
    Conta palavras positivas e negativas distintas. Vence o lado com mais palavras;
    a confianca cresce com a quantidade de evidencias, limitada a 0.99. Empate (inclusive
    nenhuma palavra conhecida) e "neutro".
    """
    palavras = tokens(texto)
    pos = len(palavras & PALAVRAS_POSITIVAS)
    neg = len(palavras & PALAVRAS_NEGATIVAS)

    if pos > neg:
        sentimento, confianca = "positivo", min(0.99, 0.70 + 0.1 * pos)
    elif neg > pos:
        sentimento, confianca = "negativo", min(0.99, 0.70 + 0.1 * neg)
    else:
        sentimento, confianca = "neutro", 0.65

    return {"sentimento": sentimento, "confianca": round(confianca, 2)}
