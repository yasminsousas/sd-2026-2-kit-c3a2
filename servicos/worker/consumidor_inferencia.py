"""
Consumidor da fila de INFERENCIA: executa os jobs assincronos de POST /predict.

O gateway publica {"job_id", "texto", "dono", "criado_em", "_trace"} em fila:inferencia
e grava resultado:<job_id> = "enfileirado". Este consumidor chama o servico de
inferencia e grava o resultado final em resultado:<job_id> (SETEX, TTL_RESULTADOS_SEG:
o resultado contem o texto classificado de um usuario e nao deve ficar para sempre - LGPD).

Mesmo padrao confiavel do consumidor de ingestao:
  - BLMOVE para fila:inferencia:processando; LREM so DEPOIS de gravar o resultado.
    Se o processo cair no meio, o job continua em "processando" e volta para a fila
    na recuperacao (ao iniciar e depois de qualquer erro do Redis): pelo menos uma vez;
  - idempotente por job_id: se o resultado final ja foi gravado (caiu entre o SETEX e o
    LREM), o job reprocessado so e confirmado, sem chamar a inferencia de novo;
  - mensagens invalidas vao para fila:inferencia:erros (MULTI/EXEC), sem travar a fila.
    LGPD: a lista de erros guarda texto de usuario, entao e limitada (LTRIM para as
    ultimas FILA_ERROS_MAX) e expira (FILA_ERROS_TTL_SEG, padrao 7 dias);
  - direito de eliminacao: o gateway grava resultado:<job_id> ("enfileirado") ANTES de
    publicar e o apaga num DELETE do usuario. Se a chave nao existe mais, o job foi
    cancelado: nem chamamos a inferencia. E o resultado final e gravado com SET XX (so
    se a chave ainda existir): um DELETE feito DURANTE o processamento tambem vale, e o
    resultado e descartado em vez de "ressuscitar" o dado apagado.

Protecao da chamada HTTP a inferencia:
  - TEMPO-LIMITE em cada chamada (WORKER_TIMEOUT);
  - RETENTATIVA com backoff exponencial para falhas transitorias (WORKER_TENTATIVAS,
    WORKER_BACKOFF); erro permanente (4xx) vira falha na hora, sem repetir;
  - CIRCUIT BREAKER: com a inferencia fora do ar, o circuito abre e o job NAO e
    perdido nem marcado como falha - volta para o INICIO da fila (mantendo a ordem) e o
    consumidor pausa ate o circuito poder testar de novo, sem girar em laco consumindo
    CPU e sem esvaziar a fila marcando tudo como falha.
Falha definitiva (retentativas esgotadas com o circuito ainda fechado, ou erro
permanente) -> resultado "falha". Se as retentativas esgotadas ABRIREM o circuito, o
servico e que esta fora: o job volta para a fila como no caso do circuito ja aberto.

Cada job vira um span CONSUMER filho do contexto que veio na mensagem: o trace do
POST no gateway continua aqui e, pelo requests instrumentado, no servico de inferencia.

Varias replicas deste consumidor podem rodar (WORKER_FILAS=inferencia), mas, como a
lista "processando" e compartilhada, uma replica que reinicia devolve tambem os jobs em
andamento das outras: eles podem ser executados duas vezes (aceitavel: idempotente).
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import redis
import requests
from opentelemetry.trace import SpanKind, Status, StatusCode

from servicos.comum import rastreamento
from servicos.comum.circuit_breaker import CircuitBreaker, CircuitoAberto
from servicos.comum.llm import erro_transitorio

FILA = os.getenv("FILA_INFERENCIA", "fila:inferencia")
FILA_PROCESSANDO = FILA + ":processando"
FILA_ERROS = FILA + ":erros"
URL_INFERENCIA = os.getenv("URL_INFERENCIA", "http://localhost:8001")
TIMEOUT = float(os.getenv("WORKER_TIMEOUT", "5"))
TENTATIVAS = int(os.getenv("WORKER_TENTATIVAS", "3"))
BACKOFF = float(os.getenv("WORKER_BACKOFF", "0.5"))   # 0,5s, 1s, 2s...
TTL_RESULTADOS_SEG = int(os.getenv("TTL_RESULTADOS_SEG", "3600"))
FILA_ERROS_MAX = int(os.getenv("FILA_ERROS_MAX", "100"))
FILA_ERROS_TTL_SEG = int(os.getenv("FILA_ERROS_TTL_SEG", "604800"))   # 7 dias
ESPERA_BLOQUEIO = 5

disjuntor = CircuitBreaker(
    limite_falhas=int(os.getenv("WORKER_LIMITE_FALHAS", "5")),
    espera_segundos=float(os.getenv("WORKER_ESPERA_CIRCUITO", "15")),
    conta_como_falha=erro_transitorio,   # 4xx prova que o servico respondeu
)

log = logging.getLogger("worker.inferencia")
tracer = rastreamento.tracer("worker.inferencia")


class Pausar(Exception):
    """O circuito esta aberto: o job foi devolvido e o consumidor deve esperar `segundos`."""

    def __init__(self, segundos: float):
        super().__init__(f"circuito aberto; pausa de {segundos:g}s")
        self.segundos = segundos


class FalhaInferencia(Exception):
    """A inferencia nao produziu resultado mesmo apos as retentativas (ou recusou o pedido)."""


def chave_resultado(job_id: str) -> str:
    return f"resultado:{job_id}"


def recuperar_pendentes(r: redis.Redis) -> int:
    """Devolve para a fila, na ordem original e atomicamente (LMOVE), o que esta em processamento."""
    devolvidas = 0
    while r.lmove(FILA_PROCESSANDO, FILA, "RIGHT", "LEFT") is not None:
        devolvidas += 1
    return devolvidas


def _decodificar(bruto: str):
    try:
        msg = json.loads(bruto)
    except ValueError:
        return None
    return msg if isinstance(msg, dict) else None


def _validar(msg) -> bool:
    return (msg is not None
            and isinstance(msg.get("job_id"), str) and msg["job_id"].strip() != ""
            and isinstance(msg.get("texto"), str) and msg["texto"].strip() != "")


def _descrever(erro: Exception) -> str:
    """Descricao curta para log e para o resultado: nunca a URL nem o texto."""
    if isinstance(erro, requests.HTTPError) and erro.response is not None:
        return f"HTTP {erro.response.status_code}"
    if isinstance(erro, requests.Timeout):
        return "tempo-limite excedido"
    if isinstance(erro, requests.ConnectionError):
        return "falha de conexao"
    if isinstance(erro, (ValueError, KeyError, TypeError)):
        return "resposta da inferencia fora do formato esperado"
    return type(erro).__name__


def _chamar_predict(texto: str) -> dict:
    r = requests.post(f"{URL_INFERENCIA}/predict", json={"texto": texto}, timeout=TIMEOUT)
    r.raise_for_status()
    corpo = r.json()
    sentimento, confianca = corpo["sentimento"], corpo["confianca"]
    if not isinstance(sentimento, str) or not isinstance(confianca, (int, float)):
        raise ValueError("formato inesperado")
    return {"sentimento": sentimento, "confianca": confianca}


def inferir(texto: str) -> dict:
    """
    Retentativa por fora do disjuntor (mesmo desenho do LLM no servico de inferencia):
    cada tentativa conta para o circuito; CircuitoAberto sobe sem ser repetido.
    """
    ultimo_erro = None
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            return disjuntor.chamar(_chamar_predict, texto)
        except CircuitoAberto:
            raise
        except Exception as erro:
            ultimo_erro = erro
            permanente = not erro_transitorio(erro)
            log.warning("inferencia falhou (tentativa %d/%d): %s%s trace_id=%s", tentativa,
                        TENTATIVAS, _descrever(erro),
                        " [erro permanente: nao sera repetido]" if permanente else "",
                        rastreamento.trace_id_atual())
            if permanente:
                raise FalhaInferencia(_descrever(erro)) from erro
            if tentativa == TENTATIVAS:
                break
            time.sleep(BACKOFF * 2 ** (tentativa - 1))
    if disjuntor.estado == "aberto":
        # as falhas deste job abriram o circuito: o problema e o servico, nao o job.
        # Trata como circuito aberto (job volta para a fila) em vez de falha definitiva
        raise CircuitoAberto("circuito abriu durante as retentativas") from ultimo_erro
    raise FalhaInferencia(_descrever(ultimo_erro)) from ultimo_erro


def _agora() -> str:
    return datetime.now(timezone.utc).isoformat()


class Worker:

    def __init__(self, r: redis.Redis):
        self.r = r
        self.recuperar = True   # ao iniciar e depois de qualquer erro do Redis

    # -- operacoes no Redis ---------------------------------------------------------
    def _gravar(self, saida: dict) -> bool:
        """
        Grava o resultado SO se a chave ainda existir (SET ... EX ttl XX). Devolve False se
        o usuario apagou o job (DELETE no gateway) enquanto ele estava na fila/em execucao.
        """
        gravou = self.r.set(chave_resultado(saida["job_id"]),
                            json.dumps(saida, ensure_ascii=False),
                            ex=TTL_RESULTADOS_SEG, xx=True)
        return bool(gravou)

    def _descartar(self, bruto: str) -> None:
        """Move a mensagem de "processando" para a fila de erros, em uma unica transacao."""
        pipe = self.r.pipeline()
        pipe.rpush(FILA_ERROS, bruto)
        pipe.ltrim(FILA_ERROS, -FILA_ERROS_MAX, -1)       # LGPD: guarda so as ultimas
        pipe.expire(FILA_ERROS, FILA_ERROS_TTL_SEG)
        pipe.lrem(FILA_PROCESSANDO, 1, bruto)
        pipe.execute()

    def _devolver(self, bruto: str) -> None:
        """Volta o job para o INICIO da fila (sera o proximo), atomicamente."""
        pipe = self.r.pipeline()
        pipe.lpush(FILA, bruto)
        pipe.lrem(FILA_PROCESSANDO, 1, bruto)
        pipe.execute()

    def _situacao(self, job_id: str) -> str:
        """"cancelado" (chave apagada), "finalizado" (ja tem resultado final) ou "pendente"."""
        bruto = self.r.get(chave_resultado(job_id))
        if bruto is None:
            return "cancelado"
        try:
            if json.loads(bruto).get("status") in ("concluido", "falha"):
                return "finalizado"
        except (ValueError, AttributeError):
            pass
        return "pendente"

    def _cancelado(self, bruto: str, job_id: str, trace_id: str) -> None:
        self.r.lrem(FILA_PROCESSANDO, 1, bruto)
        log.info("job_id=%s job cancelado (resultado apagado pelo usuario); descartado "
                 "trace_id=%s", job_id, trace_id)

    # -- processamento ----------------------------------------------------------------
    def processar(self, bruto: str) -> None:
        msg = _decodificar(bruto)
        with tracer.start_as_current_span(f"fila.consumir {FILA}",
                                          context=rastreamento.extrair(msg),
                                          kind=SpanKind.CONSUMER) as span:
            span.set_attribute("messaging.system", "redis")
            span.set_attribute("messaging.destination.name", FILA)
            trace_id = rastreamento.trace_id_atual()

            if not _validar(msg):
                log.warning("mensagem invalida (%d bytes) enviada para %s trace_id=%s",
                            len(bruto), FILA_ERROS, trace_id)
                span.set_status(Status(StatusCode.ERROR, "mensagem invalida"))
                if msg is not None and isinstance(msg.get("job_id"), str) and msg["job_id"]:
                    # quem consulta o job nao fica preso em "enfileirado" ate o TTL vencer
                    self._gravar({"job_id": msg["job_id"], "status": "falha",
                                  "dono": msg.get("dono"), "resultado": None,
                                  "detalhe": "mensagem invalida", "trace_id": trace_id,
                                  "concluido_em": _agora()})
                self._descartar(bruto)
                return

            job_id = msg["job_id"]
            span.set_attribute("messaging.message.id", job_id)
            situacao = self._situacao(job_id)
            if situacao == "cancelado":
                self._cancelado(bruto, job_id, trace_id)
                return
            if situacao == "finalizado":
                log.info("job_id=%s ja finalizado; apenas confirmando trace_id=%s",
                         job_id, trace_id)
                self.r.lrem(FILA_PROCESSANDO, 1, bruto)
                return

            inicio = time.monotonic()
            try:
                resultado = inferir(msg["texto"])
                saida = {"job_id": job_id, "status": "concluido", "dono": msg.get("dono"),
                         "resultado": resultado, "trace_id": trace_id,
                         "concluido_em": _agora()}
            except CircuitoAberto:
                self._devolver(bruto)
                log.warning("job_id=%s devolvido para a fila: circuito da inferencia aberto "
                            "trace_id=%s", job_id, trace_id)
                span.set_status(Status(StatusCode.ERROR, "circuito aberto"))
                raise Pausar(disjuntor.espera_segundos)
            except FalhaInferencia as erro:
                span.set_status(Status(StatusCode.ERROR, str(erro)))
                saida = {"job_id": job_id, "status": "falha", "dono": msg.get("dono"),
                         "resultado": None, "detalhe": f"inferencia indisponivel: {erro}",
                         "trace_id": trace_id, "concluido_em": _agora()}

            if not self._gravar(saida):
                self._cancelado(bruto, job_id, trace_id)   # DELETE durante o processamento
                return
            # so depois de gravar: se cair entre as duas linhas, o job volta e e
            # reconhecido como ja finalizado
            self.r.lrem(FILA_PROCESSANDO, 1, bruto)
            log.info("job_id=%s status=%s duracao_ms=%.0f ttl=%ds trace_id=%s", job_id,
                     saida["status"], (time.monotonic() - inicio) * 1000,
                     TTL_RESULTADOS_SEG, trace_id)

    def passo(self, espera_bloqueio: int = ESPERA_BLOQUEIO) -> int:
        """
        Uma volta do laco: consome e processa UM job. Devolve 1 se processou, 0 se a fila
        estava vazia. Levanta Pausar com o circuito aberto; outros erros sobem para quem chamou.
        """
        r = self.r
        try:
            if self.recuperar:
                devolvidas = recuperar_pendentes(r)
                if devolvidas:
                    log.info("%d job(s) pendente(s) devolvido(s) para a fila", devolvidas)
                self.recuperar = False

            bruto = r.blmove(FILA, FILA_PROCESSANDO, espera_bloqueio, "LEFT", "RIGHT")
            if bruto is None:
                return 0
            try:
                self.processar(bruto)
            except (redis.RedisError, Pausar):
                raise
            except Exception:
                log.exception("falha inesperada ao processar job; sera devolvido para a fila")
                self.recuperar = True
                raise
            return 1
        except redis.RedisError:
            self.recuperar = True
            raise
