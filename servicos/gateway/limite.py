"""
Limite de taxa (rate limit) com janela fixa de 1 minuto.

Distribuido: o contador fica no Redis (`ratelimit:<id>:<minuto>`), entao todas as replicas
do gateway enxergam o mesmo consumo - um limite em memoria seria multiplicado pelo numero
de replicas. INCR + EXPIRE vao no MESMO pipeline transacional: se o processo morrer entre
os dois comandos nao sobra contador sem expiracao.

Degradacao: se o Redis cair o gateway NAO para de atender. Passa a contar em memoria
(limite por replica, menos preciso, mas ainda protege) e loga um aviso. Um disjuntor
evita pagar o timeout do Redis em toda requisicao enquanto ele estiver fora: depois de
uma falha, o Redis so e tentado de novo apos REDIS_ESPERA_RETENTAR segundos.

Janela fixa e simples e barata (1 chave por cliente por minuto). O custo conhecido e
permitir ate 2x o limite na virada do minuto; aceitavel para este uso.
"""
import logging
import threading
import time
from dataclasses import dataclass

import redis

from servicos.comum.circuit_breaker import CircuitBreaker, CircuitoAberto

log = logging.getLogger("gateway.limite")

JANELA_SEG = 60


@dataclass
class Decisao:
    permitido: bool
    limite: int
    restante: int
    retry_after: int      # segundos ate a janela virar
    origem: str           # "redis" ou "memoria"


class LimitadorMemoria:
    """Contador por (chave, minuto) local ao processo; usado so quando o Redis esta fora."""

    def __init__(self):
        self._contadores = {}
        self._trava = threading.Lock()

    def incrementar(self, chave: str, minuto: int) -> int:
        with self._trava:
            # descarta janelas antigas para a memoria nao crescer sem fim
            for antiga in [k for k in self._contadores if k[1] < minuto]:
                del self._contadores[antiga]
            atual = self._contadores.get((chave, minuto), 0) + 1
            self._contadores[(chave, minuto)] = atual
            return atual


class Limitador:

    def __init__(self, obter_redis, espera_retentar=5.0, relogio=time.time):
        """`obter_redis` devolve o cliente atual (permite trocar o Redis nos testes)."""
        self._obter_redis = obter_redis
        self._relogio = relogio
        self.memoria = LimitadorMemoria()
        self.disjuntor = CircuitBreaker(limite_falhas=1, espera_segundos=espera_retentar)
        self._degradado = False
        self._trava = threading.Lock()

    def _incrementar_redis(self, chave: str) -> int:
        pipe = self._obter_redis().pipeline(transaction=True)
        pipe.incr(chave)
        pipe.expire(chave, JANELA_SEG + 5)   # folga: a chave sobrevive ao fim da janela
        contagem, _ = pipe.execute()
        return int(contagem)

    def _marcar(self, degradado: bool):
        with self._trava:
            if degradado == self._degradado:
                return
            self._degradado = degradado
        if degradado:
            log.warning("Redis indisponivel: rate limit em memoria (por replica) ate ele voltar")
        else:
            log.info("Redis de volta: rate limit distribuido restabelecido")

    def verificar(self, identificador: str, limite: int) -> Decisao:
        """Conta uma requisicao de `identificador` (ja pseudonimizado) e decide se passa."""
        agora = self._relogio()
        minuto = int(agora // JANELA_SEG)
        retry_after = max(1, int(JANELA_SEG - (agora % JANELA_SEG)))
        chave = f"ratelimit:{identificador}:{minuto}"
        try:
            contagem = self.disjuntor.chamar(self._incrementar_redis, chave)
            origem = "redis"
            self._marcar(False)
        except (redis.RedisError, OSError, CircuitoAberto):
            contagem = self.memoria.incrementar(chave, minuto)
            origem = "memoria"
            self._marcar(True)
        return Decisao(permitido=contagem <= limite, limite=limite,
                       restante=max(0, limite - contagem), retry_after=retry_after,
                       origem=origem)
