"""Apoio dos testes: Redis em memoria com injecao de falhas e indice em pasta temporaria."""
import contextlib
import fnmatch
import logging
import tempfile
import time
from pathlib import Path
from unittest import mock

import redis

from servicos.comum import indice


class FakeRedis:
    """
    Redis em memoria, so com os comandos que o projeto usa: listas (RPUSH/LPUSH/LLEN/
    LREM/LMOVE/BLMOVE), strings com expiracao (GET/SET/SETEX/INCR/EXPIRE/TTL/DELETE/
    EXISTS/SCAN), LTRIM e pipeline MULTI/EXEC. `falhar("lrem", 2)` faz as proximas 2 chamadas
    de LREM levantarem ConnectionError, como se o Redis tivesse caido.
    O relogio das expiracoes e injetavel (`relogio`), para testar TTL sem esperar.
    """

    def __init__(self, relogio=time.monotonic):
        self.listas = {}
        self.valores = {}
        self.expira_em = {}
        self._falhas = {}
        self._relogio = relogio

    def falhar(self, comando, vezes=1):
        self._falhas[comando] = vezes

    def _entrada(self, comando):
        if self._falhas.get(comando, 0) > 0:
            self._falhas[comando] -= 1
            raise redis.ConnectionError(f"falha injetada em {comando}")

    def _lista(self, nome):
        return self.listas.setdefault(nome, [])

    def _expirar(self, chave):
        prazo = self.expira_em.get(chave)
        if prazo is not None and self._relogio() >= prazo:
            self.valores.pop(chave, None)
            self.expira_em.pop(chave, None)

    # -- operacoes puras (sem injecao): usadas pelos comandos e pelo pipeline --------
    def _rpush(self, nome, *valores):
        lista = self._lista(nome)
        lista.extend(valores)
        return len(lista)

    def _lpush(self, nome, *valores):
        for valor in valores:
            self._lista(nome).insert(0, valor)
        return len(self._lista(nome))

    def _llen(self, nome):
        return len(self._lista(nome))

    def _ltrim(self, nome, inicio, fim):
        lista = self._lista(nome)
        n = len(lista)
        inicio = max(0, inicio + n if inicio < 0 else inicio)
        fim = fim + n if fim < 0 else fim
        lista[:] = lista[inicio:fim + 1]
        return True

    def _lrem(self, nome, contagem, valor):
        assert contagem == 1, "o projeto so remove uma ocorrencia por vez"
        lista, removidas = self._lista(nome), 0
        for i in range(len(lista)):
            if lista[i] == valor:
                del lista[i]
                removidas += 1
                break
        return removidas

    def _mover(self, origem, destino, src, dest):
        lista = self._lista(origem)
        if not lista:
            return None
        valor = lista.pop(0 if src == "LEFT" else -1)
        alvo = self._lista(destino)
        alvo.insert(0, valor) if dest == "LEFT" else alvo.append(valor)
        return valor

    def _get(self, chave):
        self._expirar(chave)
        return self.valores.get(chave)

    def _set(self, chave, valor, ex=None, nx=False, xx=False, keepttl=False):
        self._expirar(chave)
        if (nx and chave in self.valores) or (xx and chave not in self.valores):
            return None
        self.valores[chave] = str(valor)
        if ex is not None:
            self.expira_em[chave] = self._relogio() + ex
        elif not keepttl:
            self.expira_em.pop(chave, None)
        return True

    def _setex(self, chave, segundos, valor):
        return self._set(chave, valor, ex=segundos)

    def _incr(self, chave):
        self._expirar(chave)
        novo = int(self.valores.get(chave, 0)) + 1
        self.valores[chave] = str(novo)
        return novo

    def _expire(self, chave, segundos):
        self._expirar(chave)
        if chave not in self.valores and chave not in self.listas:
            return False
        self.expira_em[chave] = self._relogio() + segundos
        return True

    def _delete(self, *chaves):
        apagadas = 0
        for chave in chaves:
            self._expirar(chave)
            if self.valores.pop(chave, None) is not None:
                apagadas += 1
            self.expira_em.pop(chave, None)
            if self.listas.pop(chave, None) is not None:
                apagadas += 1
        return apagadas

    # -- comandos ---------------------------------------------------------------------
    def __getattr__(self, nome):
        """rpush, get, setex... = falha injetada (se houver) + operacao pura."""
        puro = f"_{nome}"
        if nome.startswith("_") or not hasattr(type(self), puro):
            raise AttributeError(nome)

        def comando(*args, **kwargs):
            self._entrada(nome)
            return getattr(self, puro)(*args, **kwargs)
        return comando

    def lmove(self, origem, destino, src="LEFT", dest="RIGHT"):
        self._entrada("lmove")
        return self._mover(origem, destino, src, dest)

    def blmove(self, origem, destino, timeout, src="LEFT", dest="RIGHT"):
        self._entrada("blmove")
        return self._mover(origem, destino, src, dest)

    def exists(self, *chaves):
        self._entrada("exists")
        return sum(1 for c in chaves if self._get(c) is not None or c in self.listas)

    def ttl(self, chave):
        self._entrada("ttl")
        if self._get(chave) is None:
            return -2
        prazo = self.expira_em.get(chave)
        return -1 if prazo is None else max(0, round(prazo - self._relogio()))

    def scan_iter(self, match="*", count=None):
        self._entrada("scan_iter")
        for chave in list(self.valores):
            if self._get(chave) is not None and fnmatch.fnmatchcase(chave, match):
                yield chave

    def ping(self):
        self._entrada("ping")
        return True

    def pipeline(self, transaction=True):
        return _Pipeline(self)


class _Pipeline:
    """MULTI/EXEC: ou aplica todos os comandos, ou nenhum (a falha so e injetada no EXEC)."""

    def __init__(self, r):
        self._r, self._comandos = r, []

    def __getattr__(self, nome):
        puro = getattr(self._r, f"_{nome}")

        def enfileirar(*args, **kwargs):
            self._comandos.append(lambda: puro(*args, **kwargs))
            return self
        return enfileirar

    def execute(self):
        self._r._entrada("execute")
        comandos, self._comandos = self._comandos, []
        return [comando() for comando in comandos]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def usar_indice_temporario(teste):
    """Faz o indice (servicos.comum.indice) gravar em uma pasta temporaria, removida ao fim do teste."""
    pasta = tempfile.TemporaryDirectory()
    teste.addCleanup(pasta.cleanup)
    base = Path(pasta.name)
    for alvo, valor in (("DATA_DIR", base), ("ARQUIVO", base / "indice.json")):
        patcher = mock.patch.object(indice, alvo, valor)
        patcher.start()
        teste.addCleanup(patcher.stop)
    return base


def remendar(teste, alvo, **valores):
    """mock.patch.object para varios atributos de uma vez, desfeito ao fim do teste."""
    for nome, valor in valores.items():
        patcher = mock.patch.object(alvo, nome, valor)
        patcher.start()
        teste.addCleanup(patcher.stop)


_exportador = None


def capturar_spans(teste):
    """
    Liga o rastreamento (sem exportador de rede) e captura os spans em memoria.
    O provedor global so pode ser definido uma vez por processo, entao o exportador
    em memoria e criado uma unica vez e apenas esvaziado a cada teste.
    """
    global _exportador
    from opentelemetry import trace
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    from servicos.comum import rastreamento

    rastreamento.configurar("testes")
    if _exportador is None:
        _exportador = InMemorySpanExporter()
        trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(_exportador))
    _exportador.clear()
    teste.addCleanup(_exportador.clear)
    return _exportador


@contextlib.contextmanager
def logs_ligados():
    """tests/__init__.py silencia o log da suite; os testes que verificam log religam so no bloco."""
    anterior = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    try:
        yield
    finally:
        logging.disable(anterior)
