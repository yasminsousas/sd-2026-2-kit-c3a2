"""
Worker da plataforma: UM processo, UM consumidor por fila, cada um na sua thread.

  ingestao    -> consumidor_ingestao.py   (fila:ingestao: embeddings + indice em disco)
  inferencia  -> consumidor_inferencia.py (fila:inferencia: jobs assincronos de /predict)

WORKER_FILAS escolhe quais consumidores este processo roda (padrao "inferencia,ingestao").
Por que separavel: o consumidor de ingestao e o UNICO escritor do indice (uma replica
so), enquanto o de inferencia pode ter varias replicas. No compose um processo roda os
dois; num cluster da para ter "WORKER_FILAS=ingestao" com 1 replica e
"WORKER_FILAS=inferencia" com N.

Robustez do laco:
  - queda do Redis NAO derruba o processo: o consumidor registra, espera ESPERA_ERRO
    e tenta de novo (as mensagens em andamento sao recuperadas na volta seguinte);
  - circuito da inferencia aberto: o job ja voltou para a fila e a thread so dorme
    (Event.wait, sem gastar CPU) ate o circuito poder testar de novo;
  - SIGTERM/SIGINT ligam um evento de parada: cada thread termina a volta atual e sai.
    As threads sao daemon, entao uma thread presa num BLMOVE nao segura o desligamento
    alem de ESPERA_DESLIGAR; o que ficou em "processando" volta na proxima partida.

Batimento: uma thread renova `worker:batimento:<hostname>` (SETEX 15s) a cada
BATIMENTO_INTERVALO segundos, MAS so enquanto todas as threads consumidoras estao
vivas: se uma morrer, a chave expira e o gateway deixa de contar este worker.
Fica numa thread propria (e nao no laco do consumidor) porque um job pode levar mais de
15s (timeout x retentativas) e o worker nao deve parecer morto so por estar ocupado.

SEMEAR_DOCUMENTOS=<pasta>: se o indice estiver vazio ao iniciar, indexa os .txt da
pasta (origem = nome do arquivo sem extensao) direto no indice. Usado no deploy para a
URL publica ja responder perguntas. So acontece se este processo roda a ingestao
(unico escritor do indice).

Rodar (da raiz do repo): python -m servicos.worker.worker
"""
import json
import logging
import os
import signal
import socket
import threading
import time
from pathlib import Path

import redis

from servicos.comum import indice, rastreamento
from servicos.worker import consumidor_inferencia, consumidor_ingestao
from servicos.worker.consumidor_inferencia import Pausar

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
FILAS = [f.strip() for f in os.getenv("WORKER_FILAS", "inferencia,ingestao").split(",") if f.strip()]
SEMEAR_DOCUMENTOS = os.getenv("SEMEAR_DOCUMENTOS", "").strip()
ESPERA_ERRO = float(os.getenv("WORKER_ESPERA_ERRO", "2"))   # pausa depois de uma falha
BATIMENTO_TTL = 15
BATIMENTO_INTERVALO = float(os.getenv("BATIMENTO_INTERVALO", "5"))
ESPERA_DESLIGAR = 10   # segundos esperando as threads ao receber SIGTERM
HOSTNAME = socket.gethostname()

CONSUMIDORES = {
    "ingestao": consumidor_ingestao,
    "inferencia": consumidor_inferencia,
}

log = logging.getLogger("worker")


def conectar() -> redis.Redis:
    # socket_timeout maior que a espera do BLMOVE: senao o bloqueio normal viraria erro
    return redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2,
                                socket_timeout=consumidor_ingestao.ESPERA_BLOQUEIO + 5)


def chave_batimento(hostname: str = HOSTNAME) -> str:
    return f"worker:batimento:{hostname}"


def semear(pasta: str) -> int:
    """
    Indexa os .txt de `pasta` se o indice estiver vazio. Devolve quantos documentos
    entraram. Idempotente de qualquer forma (o indice ignora repetidos).
    """
    if indice.carregar():
        log.info("semeadura ignorada: o indice ja tem documentos")
        return 0
    docs = []
    for arquivo in sorted(Path(pasta).glob("*.txt")):
        texto = arquivo.read_text(encoding="utf-8").strip()
        if indice.texto_indexavel(texto):
            docs.append({"texto": texto, "origem": arquivo.stem})
    if not docs:
        log.warning("semeadura: nenhum .txt indexavel em %s", pasta)
        return 0
    resultado = indice.adicionar(docs)
    log.info("semeadura: %d documento(s) indexado(s) de %s", resultado["novos"], pasta)
    return resultado["novos"]


def executar_consumidor(nome: str, worker, parar: threading.Event) -> None:
    """Laco de um consumidor. Nenhum erro escapa: so o evento de parada encerra a thread."""
    log.info("consumidor %s pronto em %s", nome, REDIS_URL)
    while not parar.is_set():
        try:
            worker.passo()
        except Pausar as pausa:
            parar.wait(pausa.segundos)
        except redis.RedisError as erro:
            log.warning("consumidor %s: Redis indisponivel (%s); tentando de novo",
                        nome, type(erro).__name__)
            parar.wait(ESPERA_ERRO)
        except Exception:
            parar.wait(ESPERA_ERRO)   # ja registrado por passo()
    log.info("consumidor %s encerrado", nome)


def bater(r: redis.Redis, filas: list) -> None:
    r.setex(chave_batimento(), BATIMENTO_TTL,
            json.dumps({"filas": filas, "pid": os.getpid()}))


def executar_batimento(r: redis.Redis, filas: list, threads: list,
                       parar: threading.Event) -> None:
    while not parar.is_set():
        if all(t.is_alive() for t in threads):
            try:
                bater(r, filas)
            except redis.RedisError as erro:
                log.warning("batimento falhou (%s)", type(erro).__name__)
        else:
            log.error("ha consumidor parado; batimento suspenso")
        parar.wait(BATIMENTO_INTERVALO)


def encerrar_rastreamento() -> None:
    """Exporta os spans que ainda estao no buffer antes de o processo sair."""
    try:
        from opentelemetry import trace
        trace.get_tracer_provider().shutdown()
    except Exception:
        pass


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    rastreamento.configurar("worker")

    desconhecidas = [f for f in FILAS if f not in CONSUMIDORES]
    if desconhecidas or not FILAS:
        log.error("WORKER_FILAS invalido: %r (opcoes: %s)", FILAS, ", ".join(CONSUMIDORES))
        return 2

    if SEMEAR_DOCUMENTOS:
        if "ingestao" in FILAS:
            try:
                semear(SEMEAR_DOCUMENTOS)
            except Exception:
                log.exception("semeadura falhou; seguindo sem ela")
        else:
            log.warning("SEMEAR_DOCUMENTOS ignorado: este processo nao roda a ingestao")

    parar = threading.Event()

    def ao_sinal(numero, _quadro):
        log.info("sinal %s recebido; encerrando", signal.Signals(numero).name)
        parar.set()

    signal.signal(signal.SIGTERM, ao_sinal)
    signal.signal(signal.SIGINT, ao_sinal)

    threads = []
    for nome in FILAS:
        worker = CONSUMIDORES[nome].Worker(conectar())
        t = threading.Thread(target=executar_consumidor, args=(nome, worker, parar),
                             name=f"consumidor-{nome}", daemon=True)
        t.start()
        threads.append(t)
    threading.Thread(target=executar_batimento, args=(conectar(), FILAS, threads, parar),
                     name="batimento", daemon=True).start()
    log.info("worker %s rodando: filas=%s", HOSTNAME, ",".join(FILAS))

    while not parar.wait(1):
        pass
    prazo = time.monotonic() + ESPERA_DESLIGAR   # prazo total, nao por thread
    for t in threads:
        t.join(timeout=max(0, prazo - time.monotonic()))
    encerrar_rastreamento()
    log.info("worker encerrado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
