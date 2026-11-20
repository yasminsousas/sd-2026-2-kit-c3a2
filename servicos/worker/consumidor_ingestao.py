"""
Consumidor da fila de INGESTAO: gera embeddings e persiste o indice vetorial.
Porte de C2.A2 servicos/ingestao/worker.py, agora usando servicos.comum.indice.

Entrega "pelo menos uma vez": cada mensagem e MOVIDA da fila principal para uma
lista "em processamento" (BLMOVE) e so e apagada depois que o indice foi gravado
em disco. Se o worker cair no meio, ao subir de novo ele devolve as mensagens
pendentes para a fila. Como o indice ignora documentos repetidos, reprocessar
uma mensagem nao duplica nada (a operacao e idempotente).

Recuperacao de pendentes: acontece ao iniciar E depois de QUALQUER erro do Redis.
Uma queda de conexao no meio de um lote pode deixar mensagens em "processamento"
com o worker vivo; sem esta segunda recuperacao elas ficariam presas ate um reinicio.

Mensagens invalidas (JSON invalido, sem texto, texto sem nenhuma palavra) nao
travam a fila: vao para uma lista de erros, de forma atomica (MULTI/EXEC), para
inspecao manual. LGPD: essas mensagens podem conter texto de usuario, entao a lista de
erros e LIMITADA (LTRIM: so as ultimas FILA_ERROS_MAX) e EXPIRA (FILA_ERROS_TTL_SEG,
padrao 7 dias sem novos erros) - nada fica guardado para sempre.

Rastreamento: as mensagens de um lote vem de requisicoes diferentes (traces
diferentes). Escolha: UM span CONSUMER por mensagem, filho do contexto que veio
dentro dela (campo _trace). Assim cada POST /indexar enxerga no seu proprio trace
quando o documento foi indexado. A gravacao do indice e unica para o lote, entao
os spans das mensagens de um mesmo lote se sobrepoem no tempo e terminam juntos,
depois do LREM (o atributo messaging.batch.message_count diz o tamanho do lote).

Execute UM unico consumidor de ingestao (ele e o unico escritor do arquivo de
indice; um segundo "devolveria" as mensagens em andamento do primeiro ao iniciar).
Para escalar o worker, escale so a inferencia (WORKER_FILAS=inferencia).
Requer Redis 6.2+ (BLMOVE/LMOVE).
"""
import json
import logging
import os

import redis
from opentelemetry.trace import SpanKind, Status, StatusCode

from servicos.comum import indice, rastreamento

FILA = os.getenv("FILA_INGESTAO", "fila:ingestao")
FILA_PROCESSANDO = FILA + ":processando"
FILA_ERROS = FILA + ":erros"
TAMANHO_LOTE = int(os.getenv("WORKER_TAMANHO_LOTE", "50"))
FILA_ERROS_MAX = int(os.getenv("FILA_ERROS_MAX", "100"))
FILA_ERROS_TTL_SEG = int(os.getenv("FILA_ERROS_TTL_SEG", "604800"))   # 7 dias
ESPERA_BLOQUEIO = 5   # segundos esperando mensagem antes de dar uma volta no laco

log = logging.getLogger("worker.ingestao")
tracer = rastreamento.tracer("worker.ingestao")


def recuperar_pendentes(r: redis.Redis) -> int:
    """
    Devolve para a fila, na ordem original, tudo o que esta em processamento.
    Cada mensagem e movida atomicamente (LMOVE): nunca existe uma janela em que
    ela nao esteja em nenhuma das duas listas.
    """
    devolvidas = 0
    while r.lmove(FILA_PROCESSANDO, FILA, "RIGHT", "LEFT") is not None:
        devolvidas += 1
    return devolvidas


def _validar(bruto: str):
    """Converte a mensagem em documento. Devolve None se for invalida."""
    try:
        doc = json.loads(bruto)
        texto, origem = doc["texto"], doc.get("origem", "desconhecida")
        if (isinstance(texto, str) and isinstance(origem, str)
                and indice.texto_indexavel(texto)):
            return {"texto": texto, "origem": origem}
    except (ValueError, KeyError, TypeError, AttributeError):
        pass
    return None


def _contexto(bruto: str):
    """Contexto de trace carregado na mensagem (vazio se ela nao for JSON)."""
    try:
        return rastreamento.extrair(json.loads(bruto))
    except ValueError:
        return rastreamento.extrair({})


def _descartar(r: redis.Redis, bruto: str) -> None:
    """Move a mensagem de "processando" para a fila de erros, em uma unica transacao."""
    pipe = r.pipeline()  # MULTI/EXEC: ou faz as duas coisas, ou nenhuma
    pipe.rpush(FILA_ERROS, bruto)
    pipe.ltrim(FILA_ERROS, -FILA_ERROS_MAX, -1)       # LGPD: guarda so as ultimas
    pipe.expire(FILA_ERROS, FILA_ERROS_TTL_SEG)
    pipe.lrem(FILA_PROCESSANDO, 1, bruto)
    pipe.execute()


def processar_lote(r: redis.Redis, brutos: list) -> None:
    spans = []
    try:
        validos = []
        for bruto in brutos:
            span = tracer.start_span(f"fila.consumir {FILA}", context=_contexto(bruto),
                                     kind=SpanKind.CONSUMER)
            span.set_attribute("messaging.system", "redis")
            span.set_attribute("messaging.destination.name", FILA)
            span.set_attribute("messaging.batch.message_count", len(brutos))
            spans.append(span)
            doc = _validar(bruto)
            if doc is None:
                # nunca registrar o conteudo: pode ter dado pessoal
                log.warning("mensagem invalida (%d bytes) enviada para %s", len(bruto), FILA_ERROS)
                span.set_status(Status(StatusCode.ERROR, "mensagem invalida"))
                _descartar(r, bruto)
            else:
                validos.append((bruto, doc))

        if validos:
            resultado = indice.adicionar([doc for _, doc in validos])  # grava em disco
            log.info("lote de %d documento(s): %d novo(s), indice com %d",
                     len(validos), resultado["novos"], resultado["total"])

        # so apos gravar: a mensagem deixa de estar "em processamento". Se cair aqui, o
        # reinicio devolve o que sobrou e o indice ignora o repetido.
        for bruto, _ in validos:
            r.lrem(FILA_PROCESSANDO, 1, bruto)
    except Exception as erro:
        for span in spans:
            span.set_status(Status(StatusCode.ERROR, type(erro).__name__))
        raise
    finally:
        for span in spans:
            span.end()


class Worker:

    def __init__(self, r: redis.Redis):
        self.r = r
        self.recuperar = True   # ao iniciar e depois de qualquer erro do Redis

    def passo(self, espera_bloqueio: int = ESPERA_BLOQUEIO) -> int:
        """
        Uma volta do laco: consome e processa um lote. Devolve quantas mensagens
        processou (0 se a fila estava vazia). Erros sobem para quem chamou.
        """
        r = self.r
        try:
            if self.recuperar:
                devolvidas = recuperar_pendentes(r)
                if devolvidas:
                    log.info("%d mensagem(ns) pendente(s) devolvida(s) para a fila", devolvidas)
                self.recuperar = False

            primeira = r.blmove(FILA, FILA_PROCESSANDO, espera_bloqueio, "LEFT", "RIGHT")
            if primeira is None:
                return 0
            lote = [primeira]
            while len(lote) < TAMANHO_LOTE:
                proxima = r.lmove(FILA, FILA_PROCESSANDO, "LEFT", "RIGHT")
                if proxima is None:
                    break
                lote.append(proxima)

            try:
                processar_lote(r, lote)
            except redis.RedisError:
                raise
            except Exception:
                log.exception("falha ao processar o lote; devolvendo para a fila")
                self.recuperar = True   # a proxima volta devolve o que ficou em processamento
                raise
            return len(lote)
        except redis.RedisError:
            self.recuperar = True       # o que estava em processamento volta na proxima volta
            raise
