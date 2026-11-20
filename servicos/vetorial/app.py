"""
Servico VETORIAL: armazenamento vetorial da plataforma (ingestao + recuperacao).

Funde dois servicos do C2.A2:
  - a API de ingestao: POST /indexar so ENFILEIRA os documentos (fila:ingestao) e
    responde 202. Quem gera os embeddings e grava o indice e o worker, para que quem
    envia documentos nao espere a reindexacao (custo proporcional ao indice);
  - a recuperacao: POST /buscar acha os trechos mais parecidos com a pergunta.

Por que fundir: no C2.A2 a recuperacao pedia o indice inteiro a ingestao por HTTP a
cada consulta. Aqui o indice e lido DIRETO do disco (volume compartilhado com o
worker, DATA_DIR). A escrita do worker e atomica (os.replace), entao a leitura nunca
pega um arquivo pela metade, e cada busca enxerga o que acabou de ser indexado.
Este servico NUNCA escreve no indice: o unico escritor e o worker.

Rastreamento: a requisicao HTTP vira um span (FastAPI instrumentado) e a publicacao
na fila um span PRODUCER; o contexto vai dentro de cada mensagem (campo _trace) para
o worker continuar o mesmo trace.

Rodar (da raiz do repo): uvicorn servicos.vetorial.app:app --port 8002
"""
import json
import logging
import os
from collections import Counter

import redis
from fastapi import FastAPI, HTTPException
from opentelemetry.trace import SpanKind
from pydantic import BaseModel, Field, field_validator

from servicos.comum import indice, rastreamento
from servicos.comum.embeddings import Vetorizador, similaridade

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
FILA = os.getenv("FILA_INGESTAO", "fila:ingestao")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("vetorial")

app = FastAPI(title="Vetorial - C3.A2")
tracer = rastreamento.configurar("vetorial", app)

_redis = redis.Redis.from_url(
    REDIS_URL, decode_responses=True,
    socket_connect_timeout=2, socket_timeout=5,
)


class Documento(BaseModel):
    texto: str
    origem: str = "desconhecida"

    @field_validator("texto")
    @classmethod
    def texto_nao_vazio(cls, valor: str) -> str:
        if not indice.texto_indexavel(valor):
            raise ValueError("o texto precisa ter ao menos uma palavra (2+ caracteres)")
        return valor


class Consulta(BaseModel):
    pergunta: str = Field(min_length=1)
    top_k: int = Field(default=3, ge=1, le=20)


class IndiceInvalido(Exception):
    """O arquivo do indice existe mas nao tem o formato esperado."""


def _item_valido(item) -> bool:
    return (isinstance(item, dict)
            and isinstance(item.get("texto"), str)
            and isinstance(item.get("origem"), str)
            and isinstance(item.get("vetor"), list))


def _carregar_indice() -> list:
    """
    Le o indice do disco e valida a estrutura. Um arquivo corrompido ou editado a mao
    nao pode virar 500 com stack trace: vira IndiceInvalido (503 para o cliente).
    """
    try:
        itens = indice.carregar()
    except (ValueError, OSError) as erro:
        log.error("falha ao ler o indice: %s", type(erro).__name__)
        raise IndiceInvalido() from erro
    if not isinstance(itens, list) or not all(_item_valido(i) for i in itens):
        log.error("indice em disco tem formato invalido")
        raise IndiceInvalido()
    return itens


@app.post("/indexar", status_code=202)
def indexar(docs: list[Documento]):
    """Enfileira os documentos para indexacao assincrona pelo worker."""
    if not docs:
        raise HTTPException(status_code=422, detail="envie ao menos um documento")
    with tracer.start_as_current_span(f"fila.publicar {FILA}", kind=SpanKind.PRODUCER) as span:
        span.set_attribute("messaging.system", "redis")
        span.set_attribute("messaging.destination.name", FILA)
        span.set_attribute("messaging.batch.message_count", len(docs))
        mensagens = [json.dumps(rastreamento.injetar({"texto": d.texto, "origem": d.origem}),
                                ensure_ascii=False) for d in docs]
        try:
            pipe = _redis.pipeline()
            pipe.rpush(FILA, *mensagens)
            pipe.llen(FILA)
            _, tamanho_fila = pipe.execute()
        except redis.RedisError as erro:
            log.error("fila indisponivel ao indexar (%s) trace_id=%s",
                      type(erro).__name__, rastreamento.trace_id_atual())
            raise HTTPException(status_code=503,
                                detail="fila indisponivel: tente novamente em instantes")
    log.info("indexar: %d documento(s) enfileirado(s), na_fila=%d trace_id=%s",
             len(mensagens), tamanho_fila, rastreamento.trace_id_atual())
    return {"enfileirados": len(mensagens), "na_fila": tamanho_fila}


@app.post("/buscar")
def buscar(consulta: Consulta):
    """Devolve os top_k trechos mais parecidos com a pergunta, do mais ao menos relevante."""
    try:
        itens = _carregar_indice()
    except IndiceInvalido:
        raise HTTPException(status_code=503, detail="indice vetorial invalido ou ilegivel")

    if not itens:
        return {"resultados": []}

    # O indice foi vetorizado com um vocabulario ajustado em TODOS os textos.
    # Ajustar aqui com os mesmos textos reproduz exatamente o mesmo vocabulario,
    # entao o vetor da pergunta fica no mesmo espaco dos vetores guardados.
    vetorizador = Vetorizador()
    try:
        vetorizador.ajustar([i["texto"] for i in itens])
    except ValueError:
        return {"resultados": []}   # nenhum texto do indice tem palavra: nada a buscar
    vetor_pergunta = vetorizador.vetorizar(consulta.pergunta)

    pontuados = []
    try:
        for item in itens:
            vetor = item["vetor"]
            if len(vetor) != len(vetor_pergunta):
                # vetor gravado com outro vocabulario (ex.: versoes diferentes do
                # scikit-learn): recalcula aqui em vez de comparar espacos distintos
                log.warning("vetor de %r com dimensao diferente; recalculando", item["origem"])
                vetor = vetorizador.vetorizar(item["texto"])
            pontuados.append((similaridade(vetor_pergunta, vetor), item))
    except (ValueError, TypeError):
        log.error("indice em disco tem vetores invalidos")
        raise HTTPException(status_code=503, detail="indice vetorial invalido ou ilegivel")
    # sort estavel: em empate de score, vale a ordem do indice (resultado deterministico)
    pontuados.sort(key=lambda par: par[0], reverse=True)

    # score 0 = nenhuma palavra em comum: nao e contexto util para o LLM
    resultados = [
        {"texto": item["texto"], "origem": item["origem"], "score": round(score, 4)}
        for score, item in pontuados[:consulta.top_k]
        if score > 0
    ]
    log.info("buscar: top_k=%d indice=%d resultados=%d trace_id=%s",
             consulta.top_k, len(itens), len(resultados), rastreamento.trace_id_atual())
    return {"resultados": resultados}


@app.get("/documentos")
def documentos():
    """Resumo do indice por origem. Nunca devolve textos nem vetores (podem ser sensiveis/grandes)."""
    try:
        itens = _carregar_indice()
    except IndiceInvalido:
        raise HTTPException(status_code=503, detail="indice vetorial invalido ou ilegivel")
    return {"total": len(itens), "origens": dict(Counter(i["origem"] for i in itens))}


@app.get("/saude")
def saude():
    """Sempre 200 enquanto o processo responde; os campos dizem o estado das dependencias."""
    try:
        pendentes = _redis.llen(FILA)
        fila = "ok"
    except redis.RedisError:
        pendentes = None
        fila = "indisponivel"
    try:
        total = len(_carregar_indice())
    except IndiceInvalido:
        total = None
    return {"status": "ok", "documentos": total,
            "fila": fila, "pendentes_na_fila": pendentes}
