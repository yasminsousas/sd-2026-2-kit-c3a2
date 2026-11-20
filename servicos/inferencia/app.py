"""
Servico de INFERENCIA: os "modelos" da plataforma de IA como servico.

Rotas:
  POST /predict    classificacao de sentimento (sentimento.py). Chamado pelo worker,
                   que consome os jobs assincronos da fila:inferencia.
  POST /perguntar  RAG: busca trechos no servico vetorial e chama o LLM com eles como
                   contexto (porte do servico de geracao do C2.A2).

Protecao da chamada ao LLM (dependencia externa lenta e instavel), em camadas:
  - TEMPO-LIMITE: cada chamada tem timeout (LLM_TIMEOUT, ver servicos/comum/llm.py);
  - RETENTATIVA: falhas transitorias sao repetidas com espera crescente (backoff);
  - CIRCUIT BREAKER: depois de falhas seguidas o circuito abre e as chamadas sao
    recusadas na hora, sem esperar timeout, ate o LLM dar sinal de vida.
Se o LLM nao responde, o servico NAO quebra: devolve 503 com a mensagem do problema
e ainda entrega os trechos recuperados, que ja respondem a pergunta em parte
(degradacao elegante). O servico vetorial tambem e remoto e ganha o seu disjuntor.

Toda resposta traz "trace_id" (o mesmo que aparece nos logs e no Jaeger), para quem
estiver depurando encontrar a requisicao. Nunca registramos o texto nem a pergunta.

Rodar (da raiz do repo): uvicorn servicos.inferencia.app:app --port 8001
"""
import logging
import os
import time

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from servicos.comum import llm, rastreamento
from servicos.comum.circuit_breaker import CircuitBreaker, CircuitoAberto
from servicos.comum.llm import descrever_erro, erro_transitorio, gerar
from servicos.inferencia import sentimento

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("inferencia")

app = FastAPI(title="Inferencia - C3.A2")
tracer = rastreamento.configurar("inferencia", app)

URL_VETORIAL = os.getenv("URL_VETORIAL", "http://localhost:8002")
TIMEOUT_VETORIAL = float(os.getenv("TIMEOUT_VETORIAL", "10"))

TENTATIVAS_LLM = int(os.getenv("LLM_TENTATIVAS", "3"))
BACKOFF_LLM = float(os.getenv("LLM_BACKOFF", "0.5"))  # 0,5s, 1s, 2s...

disjuntor_llm = CircuitBreaker(
    limite_falhas=int(os.getenv("LLM_LIMITE_FALHAS", "3")),
    espera_segundos=float(os.getenv("LLM_ESPERA_CIRCUITO", "20")),
    conta_como_falha=erro_transitorio,   # 400/401/403/404 nao abrem o circuito
)
disjuntor_vetorial = CircuitBreaker(
    limite_falhas=int(os.getenv("VETORIAL_LIMITE_FALHAS", "3")),
    espera_segundos=float(os.getenv("VETORIAL_ESPERA_CIRCUITO", "10")),
    conta_como_falha=erro_transitorio,
)


class EntradaInferencia(BaseModel):
    texto: str = Field(min_length=1, max_length=5000)


class Pergunta(BaseModel):
    pergunta: str = Field(min_length=1, max_length=2000)
    top_k: int = Field(default=3, ge=1, le=10)


class VetorialIndisponivel(Exception):
    pass


class LLMIndisponivel(Exception):
    """O LLM nao respondeu (timeout, conexao, 5xx...) mesmo apos as retentativas."""


class LLMRecusou(LLMIndisponivel):
    """O LLM respondeu com erro permanente (400/401/403/404...): problema de configuracao."""


# -- /predict -------------------------------------------------------------------

@app.post("/predict")
def predict(entrada: EntradaInferencia):
    inicio = time.perf_counter()
    resultado = sentimento.classificar(entrada.texto)
    ms = (time.perf_counter() - inicio) * 1000
    trace_id = rastreamento.trace_id_atual()
    log.info("predict: tamanho=%d sentimento=%s latencia_ms=%.2f trace_id=%s",
             len(entrada.texto), resultado["sentimento"], ms, trace_id)
    return {**resultado, "trace_id": trace_id, "processado_em_ms": round(ms, 2)}


# -- /perguntar -----------------------------------------------------------------

def _buscar_trechos(pergunta: str, top_k: int) -> list:
    r = requests.post(f"{URL_VETORIAL}/buscar",
                      json={"pergunta": pergunta, "top_k": top_k},
                      timeout=TIMEOUT_VETORIAL)
    r.raise_for_status()
    resultados = r.json()["resultados"]
    # resposta fora do formato vira indisponibilidade (503), nunca um 500 mais adiante
    if not isinstance(resultados, list) or not all(
            isinstance(t, dict) and isinstance(t.get("texto"), str)
            and isinstance(t.get("origem"), str) for t in resultados):
        raise ValueError("resultados fora do formato")
    return resultados


def buscar_trechos(pergunta: str, top_k: int = 3) -> list:
    try:
        return disjuntor_vetorial.chamar(_buscar_trechos, pergunta, top_k)
    except CircuitoAberto:
        raise VetorialIndisponivel("servico vetorial com circuito aberto")
    except (requests.RequestException, ValueError, KeyError, TypeError) as erro:
        raise VetorialIndisponivel(f"servico vetorial falhou: {descrever_erro(erro)}")


def chamar_llm(pergunta: str, contexto: list) -> str:
    """
    LLM com retentativa por fora do disjuntor: CADA tentativa passa pelo circuit
    breaker e conta como falha. Assim o circuito abre depressa e, uma vez aberto,
    as tentativas restantes sao recusadas na hora (CircuitoAberto nao e retentado).
    """
    ultimo_erro = None
    for tentativa in range(1, TENTATIVAS_LLM + 1):
        try:
            return disjuntor_llm.chamar(gerar, pergunta, contexto)
        except CircuitoAberto:
            raise
        except Exception as erro:
            ultimo_erro = erro
            permanente = not erro_transitorio(erro)
            log.warning("LLM falhou (tentativa %d/%d): %s%s trace_id=%s", tentativa,
                        TENTATIVAS_LLM, descrever_erro(erro),
                        " [erro permanente: nao sera repetido]" if permanente else "",
                        rastreamento.trace_id_atual())
            if permanente:
                raise LLMRecusou(descrever_erro(erro)) from erro
            if tentativa == TENTATIVAS_LLM:
                break
            time.sleep(BACKOFF_LLM * 2 ** (tentativa - 1))
    raise LLMIndisponivel(descrever_erro(ultimo_erro)) from ultimo_erro


def _fontes(trechos: list) -> list:
    """Origens dos documentos que embasaram a resposta, sem repeticao e na ordem de relevancia."""
    vistas = []
    for t in trechos:
        if t["origem"] not in vistas:
            vistas.append(t["origem"])
    return vistas


def _degradado(trace_id: str, contexto: list, fontes: list, erro: str, detalhe=None):
    corpo = {"erro": erro, "degradado": True, "contexto": contexto, "fontes": fontes,
             "trace_id": trace_id}
    if detalhe is not None:
        corpo["detalhe"] = detalhe
    return JSONResponse(status_code=503, content=corpo)


@app.post("/perguntar")
def perguntar(p: Pergunta):
    trace_id = rastreamento.trace_id_atual()
    try:
        trechos = buscar_trechos(p.pergunta, p.top_k)
    except VetorialIndisponivel as erro:
        log.error("%s trace_id=%s", erro, trace_id)
        return JSONResponse(status_code=503, content={
            "erro": "nao foi possivel consultar os documentos no momento",
            "detalhe": str(erro), "trace_id": trace_id,
        })

    contexto = [t["texto"] for t in trechos]
    fontes = _fontes(trechos)

    if not contexto:
        # sem trechos relevantes nao ha com o que fundamentar: nem chama o LLM
        return {"pergunta": p.pergunta,
                "resposta": "Nao encontrei nos documentos indexados nada relacionado a essa pergunta.",
                "contexto": [], "fontes": [], "trace_id": trace_id}

    try:
        resposta = chamar_llm(p.pergunta, contexto)
    except CircuitoAberto:
        log.warning("perguntar degradado: circuito do LLM aberto trace_id=%s", trace_id)
        return _degradado(trace_id, contexto, fontes,
                          "o servico de linguagem esta instavel; o circuito foi aberto "
                          f"e novas tentativas ocorrem em ate {disjuntor_llm.espera_segundos:g}s")
    except LLMRecusou as erro:
        return _degradado(trace_id, contexto, fontes,
                          "o servico de linguagem recusou a requisicao; verifique a "
                          "configuracao (chave, URL e modelo)", str(erro))
    except LLMIndisponivel as erro:
        return _degradado(trace_id, contexto, fontes,
                          "o servico de linguagem nao respondeu; tente novamente", str(erro))

    log.info("perguntar: trechos=%d fontes=%d trace_id=%s", len(contexto), len(fontes), trace_id)
    return {"pergunta": p.pergunta, "resposta": resposta,
            "contexto": contexto, "fontes": fontes, "trace_id": trace_id}


@app.get("/saude")
def saude():
    return {"status": "ok", "disjuntor_llm": disjuntor_llm.estado,
            "disjuntor_vetorial": disjuntor_vetorial.estado,
            "llm_modo": "real" if llm.API_KEY else "simulado"}
