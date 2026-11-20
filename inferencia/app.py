"""
Servico de Inferencia (IA como Servico).
Executa classificacao de sentimento offline e propaga o X-Trace-ID.
"""
import logging
import time
import uuid

from fastapi import FastAPI, Request
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("inferencia")

app = FastAPI(title="Servico de Inferencia - C3.A2")

PALAVRAS_POSITIVAS = {"excelente", "otimo", "bom", "rapido", "perfeito", "maravilhoso", "recomendo"}
PALAVRAS_NEGATIVAS = {"ruim", "pessimo", "lento", "erro", "falha", "horrivel", "problema"}


class EntradaInferencia(BaseModel):
    texto: str


def classificar_texto(texto: str) -> dict:
    tokens = set(texto.lower().replace(".", "").replace(",", "").split())
    pos = len(tokens.intersection(PALAVRAS_POSITIVAS))
    neg = len(tokens.intersection(PALAVRAS_NEGATIVAS))

    if pos > neg:
        sentimento = "positivo"
        confianca = min(0.99, 0.70 + 0.1 * pos)
    elif neg > pos:
        sentimento = "negativo"
        confianca = min(0.99, 0.70 + 0.1 * neg)
    else:
        sentimento = "neutro"
        confianca = 0.65

    return {"sentimento": sentimento, "confianca": round(confianca, 2)}


@app.get("/saude")
def saude():
    return {"status": "ok"}


@app.post("/predict")
def predict(entrada: EntradaInferencia, request: Request):
    inicio = time.time()
    trace_id = request.headers.get("X-Trace-ID") or str(uuid.uuid4())

    resultado = classificar_texto(entrada.texto)
    ms = (time.time() - inicio) * 1000

    logger.info(
        f"trace_id={trace_id} sentimento={resultado['sentimento']} latency_ms={ms:.2f}"
    )
    return {
        **resultado,
        "trace_id": trace_id,
        "processado_em_ms": round(ms, 2),
    }