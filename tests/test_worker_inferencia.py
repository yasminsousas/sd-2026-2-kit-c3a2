"""
Consumidor da fila de inferencia (servicos/worker/consumidor_inferencia.py): entrega
pelo menos uma vez, retentativa com backoff, circuit breaker que devolve o job, mensagens
invalidas, recuperacao de pendentes, resultado com TTL e direito de eliminacao.
"""
import json
import os
import unittest
from unittest import mock

import redis
import requests
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from servicos.comum import llm
from servicos.comum.circuit_breaker import CircuitBreaker
from servicos.worker import consumidor_inferencia as ci
from tests.util import FakeRedis, capturar_spans, logs_ligados, remendar


class Relogio:
    agora = 100.0

    def __call__(self):
        return self.agora


def resposta(corpo=None, codigo=200):
    r = requests.Response()
    r.status_code = codigo
    r._content = json.dumps(corpo if corpo is not None else {}).encode()
    return r


def ok(sentimento="positivo", confianca=0.8):
    return resposta({"sentimento": sentimento, "confianca": confianca,
                     "trace_id": "x", "processado_em_ms": 0.1})


def http(codigo):
    return resposta({"detail": "erro"}, codigo)


class Base(unittest.TestCase):

    def setUp(self):
        self.r = FakeRedis()
        sufixo = os.urandom(4).hex()
        self.relogio = Relogio()
        self.disjuntor = CircuitBreaker(limite_falhas=3, espera_segundos=15,
                                        conta_como_falha=llm.erro_transitorio,
                                        relogio=self.relogio)
        remendar(self, ci, FILA=f"t:{sufixo}", FILA_PROCESSANDO=f"t:{sufixo}:processando",
                 FILA_ERROS=f"t:{sufixo}:erros", TENTATIVAS=3, BACKOFF=0.5,
                 TTL_RESULTADOS_SEG=3600, disjuntor=self.disjuntor)
        pausa = mock.patch.object(ci.time, "sleep")
        self.pausas = pausa.start()
        self.addCleanup(pausa.stop)
        self.w = ci.Worker(self.r)

    def predict_com(self, *efeitos):
        """Substitui requests.post: cada chamada devolve/levanta o proximo efeito."""
        efeito = efeitos[0] if len(efeitos) == 1 and callable(efeitos[0]) else list(efeitos)
        patcher = mock.patch.object(ci.requests, "post", side_effect=efeito)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def enviar(self, job_id="j1", texto="produto excelente", dono="hash-do-dono", **extra):
        """Como o gateway: grava 'enfileirado' ANTES de publicar."""
        self.r.setex(f"resultado:{job_id}", 3600,
                     json.dumps({"job_id": job_id, "status": "enfileirado", "dono": dono}))
        corpo = {"job_id": job_id, "texto": texto, "dono": dono,
                 "criado_em": "2026-11-19T00:00:00+00:00", **extra}
        bruto = json.dumps(corpo)
        self.r.rpush(ci.FILA, bruto)
        return bruto

    def resultado(self, job_id="j1"):
        bruto = self.r.get(f"resultado:{job_id}")
        return None if bruto is None else json.loads(bruto)

    def tamanhos(self):
        return (self.r.llen(ci.FILA), self.r.llen(ci.FILA_PROCESSANDO), self.r.llen(ci.FILA_ERROS))


class Sucesso(Base):

    def test_grava_resultado_concluido_com_dono_e_ttl_e_confirma(self):
        post = self.predict_com(ok("positivo", 0.8))
        self.enviar()
        self.assertEqual(self.w.passo(espera_bloqueio=1), 1)
        res = self.resultado()
        self.assertEqual(res["status"], "concluido")
        self.assertEqual(res["dono"], "hash-do-dono")
        self.assertEqual(res["resultado"], {"sentimento": "positivo", "confianca": 0.8})
        self.assertIn("concluido_em", res)
        self.assertIn("trace_id", res)
        self.assertEqual(self.r.ttl("resultado:j1"), 3600)
        self.assertEqual(self.tamanhos(), (0, 0, 0))
        args, kwargs = post.call_args
        self.assertTrue(args[0].endswith("/predict"))
        self.assertEqual(kwargs["json"], {"texto": "produto excelente"})
        self.assertEqual(kwargs["timeout"], ci.TIMEOUT)

    def test_resultado_expira_depois_do_ttl(self):
        relogio = Relogio()
        self.r = FakeRedis(relogio=relogio)
        self.w = ci.Worker(self.r)
        self.predict_com(ok())
        self.enviar()
        self.w.passo(espera_bloqueio=1)
        relogio.agora += 3599
        self.assertIsNotNone(self.resultado())
        relogio.agora += 1
        self.assertIsNone(self.resultado())

    def test_fila_vazia_nao_faz_nada(self):
        post = self.predict_com()
        self.assertEqual(self.w.passo(espera_bloqueio=1), 0)
        post.assert_not_called()

    def test_jobs_processados_na_ordem(self):
        self.predict_com(ok("positivo"), ok("negativo"))
        self.enviar("a")
        self.enviar("b")
        self.w.passo(espera_bloqueio=1)
        self.w.passo(espera_bloqueio=1)
        self.assertEqual((self.resultado("a")["resultado"]["sentimento"],
                          self.resultado("b")["resultado"]["sentimento"]), ("positivo", "negativo"))

    def test_log_nao_traz_o_texto(self):
        self.predict_com(ok())
        self.enviar(texto="meu segredo pessoal")
        with logs_ligados(), self.assertLogs("worker.inferencia", "INFO") as logs:
            self.w.passo(espera_bloqueio=1)
        self.assertNotIn("segredo", "\n".join(logs.output))
        self.assertIn("job_id=j1", "\n".join(logs.output))


class Retentativa(Base):

    def test_falha_transitoria_e_repetida_com_backoff_exponencial(self):
        post = self.predict_com(requests.Timeout(), requests.ConnectionError(), ok())
        self.enviar()
        self.w.passo(espera_bloqueio=1)
        self.assertEqual(post.call_count, 3)
        self.assertEqual([c.args[0] for c in self.pausas.call_args_list], [0.5, 1.0])
        self.assertEqual(self.resultado()["status"], "concluido")
        self.assertEqual(self.disjuntor.falhas, 0)

    def test_5xx_e_resposta_quebrada_tambem_sao_repetidos(self):
        post = self.predict_com(http(503), resposta({"outra": 1}), ok())
        self.enviar()
        self.w.passo(espera_bloqueio=1)
        self.assertEqual(post.call_count, 3)
        self.assertEqual(self.resultado()["status"], "concluido")

    def test_retentativas_esgotadas_viram_falha_definitiva(self):
        self.disjuntor.limite_falhas = 99      # isola a retentativa do circuito
        self.predict_com(http(500), http(500), http(500))
        self.enviar()
        self.w.passo(espera_bloqueio=1)
        res = self.resultado()
        self.assertEqual((res["status"], res["resultado"], res["dono"]),
                         ("falha", None, "hash-do-dono"))
        self.assertIn("HTTP 500", res["detalhe"])
        self.assertEqual(self.r.ttl("resultado:j1"), 3600)
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_erro_permanente_nao_e_repetido_nem_abre_o_circuito(self):
        post = self.predict_com(*[http(422)] * 5)
        for i in range(5):
            self.enviar(f"j{i}")
            self.w.passo(espera_bloqueio=1)
            self.assertEqual(self.resultado(f"j{i}")["status"], "falha")
        self.assertEqual(post.call_count, 5)
        self.assertEqual(self.disjuntor.estado, "fechado")
        self.pausas.assert_not_called()


class CircuitoAberto(Base):

    def test_circuito_aberto_devolve_o_job_para_o_inicio_da_fila_e_pede_pausa(self):
        post = self.predict_com(*[requests.ConnectionError()] * 3)
        primeiro = self.enviar("j1")
        self.enviar("j2")
        with self.assertRaises(ci.Pausar) as ctx:
            self.w.passo(espera_bloqueio=1)
        self.assertEqual(ctx.exception.segundos, 15)
        self.assertEqual(self.disjuntor.estado, "aberto")
        # o job nao foi perdido nem marcado como falha: e o proximo da fila
        self.assertEqual(self.r.listas[ci.FILA][0], primeiro)
        self.assertEqual(self.tamanhos(), (2, 0, 0))
        self.assertEqual(self.resultado()["status"], "enfileirado")
        self.assertEqual(post.call_count, 3)

    def test_com_circuito_aberto_nao_chama_a_inferencia_e_nao_consome_a_fila(self):
        self.predict_com(*[requests.ConnectionError()] * 3)
        self.enviar()
        with self.assertRaises(ci.Pausar):
            self.w.passo(espera_bloqueio=1)
        post = self.predict_com()
        for _ in range(3):
            with self.assertRaises(ci.Pausar):
                self.w.passo(espera_bloqueio=1)
        post.assert_not_called()
        self.assertEqual(self.tamanhos(), (1, 0, 0))

    def test_quando_a_inferencia_volta_o_job_devolvido_e_concluido(self):
        self.predict_com(*[requests.ConnectionError()] * 3)
        self.enviar()
        with self.assertRaises(ci.Pausar):
            self.w.passo(espera_bloqueio=1)
        self.relogio.agora += 15                 # passou a pausa: o circuito testa de novo
        self.predict_com(ok())
        self.assertEqual(self.w.passo(espera_bloqueio=1), 1)
        self.assertEqual(self.resultado()["status"], "concluido")
        self.assertEqual(self.disjuntor.estado, "fechado")
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_devolucao_e_atomica(self):
        self.predict_com(*[requests.ConnectionError()] * 3)
        self.enviar()
        self.r.falhar("execute")                 # o Redis cai justo na devolucao
        with self.assertRaises(redis.RedisError):
            self.w.passo(espera_bloqueio=1)
        self.assertEqual(self.tamanhos(), (0, 1, 0))     # segue em processamento, nao some
        with self.assertRaises(ci.Pausar):
            self.w.passo(espera_bloqueio=1)              # recupera e (circuito aberto) devolve
        self.assertEqual(self.tamanhos(), (1, 0, 0))


class MensagensInvalidas(Base):

    def test_invalidas_vao_para_a_fila_de_erros_sem_travar(self):
        post = self.predict_com(ok())
        invalidas = ["nao e json", json.dumps([1]), json.dumps({"texto": "sem job"}),
                     json.dumps({"job_id": "", "texto": "x"}), json.dumps({"job_id": 5, "texto": "x"}),
                     json.dumps({"job_id": "k", "texto": "  "}), json.dumps({"job_id": "k2"})]
        self.r.rpush(ci.FILA, *invalidas)
        self.enviar("bom")
        while self.w.passo(espera_bloqueio=1):
            pass
        self.assertEqual(self.tamanhos(), (0, 0, len(invalidas)))
        self.assertEqual(self.resultado("bom")["status"], "concluido")
        self.assertEqual(post.call_count, 1)

    def test_invalida_com_job_id_marca_falha_se_o_job_existir(self):
        self.r.setex("resultado:k", 3600, json.dumps({"job_id": "k", "status": "enfileirado",
                                                      "dono": "d"}))
        self.r.rpush(ci.FILA, json.dumps({"job_id": "k", "texto": "", "dono": "d"}))
        self.w.passo(espera_bloqueio=1)
        res = self.resultado("k")
        self.assertEqual((res["status"], res["detalhe"], res["dono"]),
                         ("falha", "mensagem invalida", "d"))

    def test_fila_de_erros_e_limitada_e_expira(self):
        with mock.patch.object(ci, "FILA_ERROS_MAX", 2):
            self.r.rpush(ci.FILA, "a", "b", "c")
            while self.w.passo(espera_bloqueio=1):
                pass
        self.assertEqual(self.r.listas[ci.FILA_ERROS], ["b", "c"])
        self.assertIn(ci.FILA_ERROS, self.r.expira_em)

    def test_log_da_invalida_nao_traz_o_conteudo(self):
        self.r.rpush(ci.FILA, "texto pessoal sem formato")
        with logs_ligados(), self.assertLogs("worker.inferencia", "WARNING") as logs:
            self.w.passo(espera_bloqueio=1)
        self.assertNotIn("pessoal", "\n".join(logs.output))


class RecuperacaoEIdempotencia(Base):

    def test_reiniciado_devolve_pendentes_na_ordem_e_processa(self):
        self.predict_com(ok(), ok(), ok())
        self.enviar("a")
        self.enviar("b")
        self.enviar("c")
        self.r.lmove(ci.FILA, ci.FILA_PROCESSANDO)   # outro processo pegou "a" e "b" e morreu
        self.r.lmove(ci.FILA, ci.FILA_PROCESSANDO)
        novo = ci.Worker(self.r)
        ordem = []
        for _ in range(3):
            novo.passo(espera_bloqueio=1)
            ordem.append(next(j for j in "abc" if self.resultado(j)["status"] == "concluido"
                              and j not in ordem))
        self.assertEqual(ordem, ["a", "b", "c"])
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_queda_do_redis_ao_gravar_mantem_o_job_e_recupera_sem_reiniciar(self):
        self.predict_com(ok(), ok())
        self.enviar()
        self.r.falhar("set")
        with self.assertRaises(redis.RedisError):
            self.w.passo(espera_bloqueio=1)
        self.assertEqual(self.tamanhos(), (0, 1, 0))
        self.assertEqual(self.w.passo(espera_bloqueio=1), 1)
        self.assertEqual(self.resultado()["status"], "concluido")
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_queda_entre_gravar_e_confirmar_nao_chama_a_inferencia_de_novo(self):
        post = self.predict_com(ok())
        self.enviar()
        self.r.falhar("lrem")
        with self.assertRaises(redis.RedisError):
            self.w.passo(espera_bloqueio=1)
        self.assertEqual(self.resultado()["status"], "concluido")
        self.assertEqual(self.w.passo(espera_bloqueio=1), 1)     # devolvido e so confirmado
        self.assertEqual(post.call_count, 1)
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_erro_inesperado_marca_recuperacao(self):
        self.enviar()
        with mock.patch.object(ci, "inferir", side_effect=RuntimeError("bug")):
            with self.assertRaises(RuntimeError):
                self.w.passo(espera_bloqueio=1)
        self.assertTrue(self.w.recuperar)
        self.predict_com(ok())
        self.w.passo(espera_bloqueio=1)
        self.assertEqual(self.resultado()["status"], "concluido")


class DireitoDeEliminacao(Base):
    """DELETE no gateway apaga resultado:<job_id>; o worker nao pode recria-lo."""

    def test_job_apagado_antes_de_processar_nem_chama_a_inferencia(self):
        post = self.predict_com(ok())
        self.enviar()
        self.r.delete("resultado:j1")
        with logs_ligados(), self.assertLogs("worker.inferencia", "INFO") as logs:
            self.assertEqual(self.w.passo(espera_bloqueio=1), 1)
        post.assert_not_called()
        self.assertIsNone(self.resultado())
        self.assertEqual(self.tamanhos(), (0, 0, 0))
        self.assertIn("job cancelado", "\n".join(logs.output))

    def test_job_apagado_durante_o_processamento_descarta_o_resultado(self):
        self.enviar()

        def apaga_e_responde(*_a, **_k):
            self.r.delete("resultado:j1")         # DELETE chega enquanto a inferencia roda
            return ok()

        self.predict_com(apaga_e_responde)
        self.w.passo(espera_bloqueio=1)
        self.assertIsNone(self.resultado())
        self.assertEqual(self.tamanhos(), (0, 0, 0))

    def test_falha_tambem_nao_recria_job_apagado(self):
        self.disjuntor.limite_falhas = 99
        self.enviar()

        def apaga_e_falha(*_a, **_k):
            self.r.delete("resultado:j1")
            raise requests.Timeout()

        self.predict_com(apaga_e_falha)
        self.w.passo(espera_bloqueio=1)
        self.assertIsNone(self.resultado())


class Rastreamento(Base):

    def test_span_consumidor_continua_o_trace_da_mensagem(self):
        spans = capturar_spans(self)
        self.predict_com(ok())
        from servicos.comum import rastreamento
        with trace.get_tracer("teste").start_as_current_span("gateway") as raiz:
            contexto = rastreamento.injetar({})["_trace"]
        self.enviar(_trace=contexto)
        self.w.passo(espera_bloqueio=1)
        [consumidor] = [s for s in spans.get_finished_spans() if s.kind == SpanKind.CONSUMER]
        self.assertEqual(consumidor.context.trace_id, raiz.get_span_context().trace_id)
        self.assertEqual(consumidor.parent.span_id, raiz.get_span_context().span_id)
        self.assertEqual(self.resultado()["trace_id"],
                         format(raiz.get_span_context().trace_id, "032x"))


if __name__ == "__main__":
    unittest.main()
