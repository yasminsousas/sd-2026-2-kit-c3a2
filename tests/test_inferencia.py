"""
Servico de inferencia: /perguntar (porte dos testes de geracao do C2.A2), /predict,
o classificador de sentimento e /saude.
"""
import json
import unittest
from unittest import mock

import requests

import servicos.inferencia.app as g
from servicos.inferencia import sentimento
from servicos.comum import llm
from servicos.comum.circuit_breaker import CircuitBreaker, CircuitoAberto

URL_COM_CHAVE = "https://llm.exemplo/v1?key=SEGREDO"


def http(codigo):
    resposta = requests.Response()
    resposta.status_code = codigo
    return requests.HTTPError(f"{codigo} Error for url: {URL_COM_CHAVE}", response=resposta)


class Relogio:
    agora = 100.0

    def __call__(self):
        return self.agora


class Base(unittest.TestCase):

    def setUp(self):
        self.relogio = Relogio()
        self.disjuntor = CircuitBreaker(limite_falhas=3, espera_segundos=20,
                                        conta_como_falha=llm.erro_transitorio,
                                        relogio=self.relogio)
        for alvo, valor in (("disjuntor_llm", self.disjuntor), ("TENTATIVAS_LLM", 3),
                            ("BACKOFF_LLM", 0.5)):
            patcher = mock.patch.object(g, alvo, valor)
            patcher.start()
            self.addCleanup(patcher.stop)
        pausa = mock.patch.object(g.time, "sleep")
        self.pausas = pausa.start()
        self.addCleanup(pausa.stop)

    def llm_com(self, *efeitos):
        """Substitui gerar(): cada chamada devolve/levanta o proximo efeito."""
        patcher = mock.patch.object(g, "gerar", side_effect=list(efeitos))
        self.addCleanup(patcher.stop)
        return patcher.start()


class ClassificacaoDeErros(unittest.TestCase):

    def test_classificacao(self):
        for codigo in (400, 401, 403, 404, 422):
            self.assertFalse(llm.erro_transitorio(http(codigo)), codigo)
        for codigo in (408, 429, 500, 502, 503, 504):
            self.assertTrue(llm.erro_transitorio(http(codigo)), codigo)
        for erro in (requests.Timeout(), requests.ConnectionError(), requests.ConnectTimeout(),
                     KeyError("choices"), ValueError("json"), IndexError()):
            self.assertTrue(llm.erro_transitorio(erro), type(erro).__name__)
        for erro in (requests.exceptions.MissingSchema(), requests.exceptions.InvalidURL(),
                     requests.exceptions.InvalidSchema()):
            self.assertFalse(llm.erro_transitorio(erro), type(erro).__name__)

    def test_descricao_nunca_traz_a_url(self):
        for erro in (http(401), http(500), requests.Timeout(URL_COM_CHAVE),
                     requests.ConnectionError(URL_COM_CHAVE), KeyError(URL_COM_CHAVE),
                     RuntimeError(URL_COM_CHAVE)):
            self.assertNotIn("SEGREDO", llm.descrever_erro(erro))
        self.assertEqual(llm.descrever_erro(http(401)), "HTTP 401")


class ErrosPermanentes(Base):

    def test_400_401_403_404_sem_retentativa_e_sem_abrir_o_circuito(self):
        for codigo in (400, 401, 403, 404):
            gerar = self.llm_com(*[http(codigo)] * 10)
            for _ in range(5):                       # bem acima do limite de 3 falhas
                with self.assertRaises(g.LLMRecusou) as ctx:
                    g.chamar_llm("p", ["c"])
                self.assertEqual(str(ctx.exception), f"HTTP {codigo}")
            self.assertEqual(gerar.call_count, 5, "uma unica tentativa por pergunta")
            self.assertEqual(self.disjuntor.estado, "fechado")
            self.assertEqual(self.disjuntor.falhas, 0)
        self.pausas.assert_not_called()

    def test_com_circuito_ja_aberto_o_erro_permanente_nao_e_recusado_como_instabilidade(self):
        self.llm_com(http(500), http(500), http(500))
        with self.assertRaises(g.LLMIndisponivel):
            g.chamar_llm("p", ["c"])
        self.assertEqual(self.disjuntor.estado, "aberto")
        self.relogio.agora += 20
        self.llm_com(http(401))                      # a sonda recebe 401: o servico voltou
        with self.assertRaises(g.LLMRecusou):
            g.chamar_llm("p", ["c"])
        self.assertEqual(self.disjuntor.estado, "fechado")


class ErrosTransitorios(Base):

    def test_408_429_5xx_timeout_e_conexao_sao_repetidos_e_contam(self):
        for erro in (http(408), http(429), http(500), http(502), http(503),
                     requests.Timeout(), requests.ConnectionError(), KeyError("choices")):
            self.disjuntor.__init__(limite_falhas=99, espera_segundos=20,
                                    conta_como_falha=llm.erro_transitorio)
            gerar = self.llm_com(erro, erro, erro)
            with self.assertRaises(g.LLMIndisponivel) as ctx:
                g.chamar_llm("p", ["c"])
            self.assertNotIsInstance(ctx.exception, g.LLMRecusou)
            self.assertEqual(gerar.call_count, 3, type(erro).__name__)
            self.assertEqual(self.disjuntor.falhas, 3)

    def test_backoff_exponencial_entre_tentativas(self):
        self.llm_com(http(503), http(503), http(503))
        with self.assertRaises(g.LLMIndisponivel):
            g.chamar_llm("p", ["c"])
        self.assertEqual([c.args[0] for c in self.pausas.call_args_list], [0.5, 1.0])

    def test_recupera_na_segunda_tentativa(self):
        self.llm_com(requests.Timeout(), "resposta")
        self.assertEqual(g.chamar_llm("p", ["c"]), "resposta")
        self.assertEqual(self.disjuntor.falhas, 0)

    def test_falhas_transitorias_seguidas_abrem_o_circuito_e_depois_recusa_sem_chamar(self):
        gerar = self.llm_com(*[http(500)] * 3)
        with self.assertRaises(g.LLMIndisponivel):
            g.chamar_llm("p", ["c"])
        self.assertEqual(self.disjuntor.estado, "aberto")
        with self.assertRaises(CircuitoAberto):
            g.chamar_llm("p", ["c"])
        self.assertEqual(gerar.call_count, 3, "com o circuito aberto o LLM nem e chamado")

    def test_circuito_fecha_quando_o_llm_volta(self):
        self.llm_com(*[http(500)] * 3)
        with self.assertRaises(g.LLMIndisponivel):
            g.chamar_llm("p", ["c"])
        self.relogio.agora += 20
        self.llm_com("de volta")
        self.assertEqual(g.chamar_llm("p", ["c"]), "de volta")
        self.assertEqual(self.disjuntor.estado, "fechado")


class RespostaDaApi(Base):
    TRECHOS = [{"texto": "t1", "origem": "doc_a", "score": 0.9},
               {"texto": "t2", "origem": "doc_a", "score": 0.5},
               {"texto": "t3", "origem": "doc_b", "score": 0.2}]

    def perguntar(self, trechos=None):
        with mock.patch.object(g, "buscar_trechos",
                               return_value=self.TRECHOS if trechos is None else trechos):
            return g.perguntar(g.Pergunta(pergunta="o que e?"))

    def corpo(self, resposta):
        return json.loads(resposta.body)

    def test_resposta_normal_traz_contexto_e_fontes_sem_repeticao(self):
        self.llm_com("42")
        r = self.perguntar()
        self.assertEqual(r["resposta"], "42")
        self.assertEqual(r["contexto"], ["t1", "t2", "t3"])
        self.assertEqual(r["fontes"], ["doc_a", "doc_b"])

    def test_llm_fora_do_ar_degrada_com_503_e_mantem_os_trechos(self):
        self.llm_com(*[http(500)] * 3)
        r = self.perguntar()
        corpo = self.corpo(r)
        self.assertEqual(r.status_code, 503)
        self.assertTrue(corpo["degradado"])
        self.assertEqual(corpo["contexto"], ["t1", "t2", "t3"])
        self.assertEqual(corpo["fontes"], ["doc_a", "doc_b"])
        self.assertNotIn("SEGREDO", r.body.decode())     # a URL com a chave nao vaza

    def test_chave_invalida_explica_que_e_configuracao_e_nao_instabilidade(self):
        self.llm_com(http(401))
        r = self.perguntar()
        corpo = self.corpo(r)
        self.assertEqual(r.status_code, 503)
        self.assertIn("configura", corpo["erro"])
        self.assertEqual(corpo["detalhe"], "HTTP 401")
        self.assertEqual(self.disjuntor.estado, "fechado")

    def test_circuito_aberto_responde_503_degradado_sem_chamar_o_llm(self):
        self.llm_com(*[http(500)] * 3)
        self.perguntar()
        gerar = self.llm_com()
        r = self.perguntar()
        self.assertEqual(r.status_code, 503)
        self.assertIn("circuito", self.corpo(r)["erro"])
        self.assertTrue(self.corpo(r)["degradado"])
        gerar.assert_not_called()

    def test_sem_trechos_nao_chama_o_llm(self):
        gerar = self.llm_com()
        r = self.perguntar(trechos=[])
        self.assertEqual((r["contexto"], r["fontes"]), ([], []))
        gerar.assert_not_called()

    def test_vetorial_indisponivel_responde_503(self):
        gerar = self.llm_com()
        with mock.patch.object(g, "buscar_trechos",
                               side_effect=g.VetorialIndisponivel("fora")):
            r = g.perguntar(g.Pergunta(pergunta="x"))
        self.assertEqual(r.status_code, 503)
        gerar.assert_not_called()


class DisjuntorDoVetorial(Base):

    def setUp(self):
        super().setUp()
        d = CircuitBreaker(limite_falhas=3, espera_segundos=10,
                           conta_como_falha=llm.erro_transitorio, relogio=self.relogio)
        patcher = mock.patch.object(g, "disjuntor_vetorial", d)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.d = d

    def test_servico_fora_abre_o_circuito_e_depois_recusa_sem_rede(self):
        with mock.patch.object(g.requests, "post", side_effect=requests.ConnectionError()) as post:
            for _ in range(3):
                with self.assertRaises(g.VetorialIndisponivel):
                    g.buscar_trechos("p")
            with self.assertRaises(g.VetorialIndisponivel):
                g.buscar_trechos("p")
        self.assertEqual(post.call_count, 3)
        self.assertEqual(self.d.estado, "aberto")


class RespostaTraceId(Base):

    def test_respostas_trazem_trace_id_inclusive_degradadas(self):
        from opentelemetry import trace
        tracer = trace.get_tracer("teste")
        trechos = [{"texto": "t1", "origem": "a", "score": 0.5}]
        with tracer.start_as_current_span("req") as span, \
                mock.patch.object(g, "buscar_trechos", return_value=trechos):
            esperado = format(span.get_span_context().trace_id, "032x")
            self.llm_com("ok")
            self.assertEqual(g.perguntar(g.Pergunta(pergunta="x"))["trace_id"], esperado)
            self.llm_com(*[http(500)] * 3)
            r = g.perguntar(g.Pergunta(pergunta="x"))
            self.assertEqual(json.loads(r.body)["trace_id"], esperado)
        with mock.patch.object(g, "buscar_trechos", side_effect=g.VetorialIndisponivel("x")):
            self.assertIn("trace_id", json.loads(g.perguntar(g.Pergunta(pergunta="x")).body))

    def test_top_k_e_repassado_ao_vetorial(self):
        d = CircuitBreaker(limite_falhas=3, espera_segundos=10)
        corpo = requests.Response()
        corpo.status_code = 200
        corpo._content = b'{"resultados": []}'
        with mock.patch.object(g, "disjuntor_vetorial", d), \
                mock.patch.object(g.requests, "post", return_value=corpo) as post:
            r = g.perguntar(g.Pergunta(pergunta="o que e cap?", top_k=7))
        self.assertEqual(post.call_args.kwargs["json"], {"pergunta": "o que e cap?", "top_k": 7})
        self.assertTrue(post.call_args.args[0].endswith("/buscar"))
        self.assertEqual(r["fontes"], [])

    def test_vetorial_com_resposta_fora_do_formato_e_indisponivel(self):
        d = CircuitBreaker(limite_falhas=3, espera_segundos=10)
        for conteudo in (b"html", b'{"outra": 1}', b'{"resultados": 5}',
                         b'{"resultados": [{"texto": 1}]}', b'{"resultados": ["x"]}'):
            corpo = requests.Response()
            corpo.status_code = 200
            corpo._content = conteudo
            with mock.patch.object(g, "disjuntor_vetorial", d), \
                    mock.patch.object(g.requests, "post", return_value=corpo):
                with self.assertRaises(g.VetorialIndisponivel, msg=conteudo):
                    g.buscar_trechos("p")


class Validacao(unittest.TestCase):

    def test_limites_de_entrada(self):
        from pydantic import ValidationError
        for kwargs in ({"pergunta": ""}, {"pergunta": "x" * 2001}, {"pergunta": "x", "top_k": 0},
                       {"pergunta": "x", "top_k": 11}):
            with self.assertRaises(ValidationError):
                g.Pergunta(**kwargs)
        for texto in ("", "x" * 5001):
            with self.assertRaises(ValidationError):
                g.EntradaInferencia(texto=texto)
        g.Pergunta(pergunta="x" * 2000, top_k=10)
        g.EntradaInferencia(texto="x" * 5000)


class Sentimento(unittest.TestCase):

    def test_positivo_negativo_e_neutro(self):
        self.assertEqual(sentimento.classificar("Produto excelente, recomendo")["sentimento"], "positivo")
        self.assertEqual(sentimento.classificar("Atendimento ruim e lento")["sentimento"], "negativo")
        self.assertEqual(sentimento.classificar("Chegou na terca")["sentimento"], "neutro")
        self.assertEqual(sentimento.classificar("bom mas lento")["sentimento"], "neutro")   # empate

    def test_acentos_e_maiusculas_nao_atrapalham(self):
        for texto in ("ÓTIMO", "ótimo", "otimo", "Péssimo", "horrível"):
            self.assertNotEqual(sentimento.classificar(texto)["sentimento"], "neutro", texto)

    def test_pontuacao_colada_na_palavra(self):
        self.assertEqual(sentimento.classificar("bom!")["sentimento"], "positivo")
        self.assertEqual(sentimento.classificar("(ruim?)")["sentimento"], "negativo")
        self.assertEqual(sentimento.classificar("lento...")["sentimento"], "negativo")
        self.assertEqual(sentimento.classificar("excelente;rapido")["confianca"], 0.9)

    def test_confianca(self):
        self.assertEqual(sentimento.classificar("bom")["confianca"], 0.8)
        self.assertEqual(sentimento.classificar("bom bom bom")["confianca"], 0.8)   # palavras distintas
        self.assertEqual(sentimento.classificar(
            "bom otimo excelente perfeito maravilhoso")["confianca"], 0.99)
        self.assertEqual(sentimento.classificar("nada")["confianca"], 0.65)


class Predict(unittest.TestCase):

    def test_predict_no_formato_do_contrato(self):
        r = g.predict(g.EntradaInferencia(texto="servico rapido"))
        self.assertEqual(set(r), {"sentimento", "confianca", "trace_id", "processado_em_ms"})
        self.assertEqual(r["sentimento"], "positivo")

    def test_via_http_com_trace_id_e_sem_texto_no_log(self):
        from fastapi.testclient import TestClient
        from tests.util import logs_ligados
        cliente = TestClient(g.app)
        with logs_ligados(), self.assertLogs("inferencia", "INFO") as logs:
            r = cliente.post("/predict", json={"texto": "meu segredo e excelente"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["trace_id"]), 32)
        self.assertNotIn("segredo", "\n".join(logs.output))
        self.assertEqual(cliente.post("/predict", json={"texto": ""}).status_code, 422)

    def test_saude(self):
        s = g.saude()
        self.assertEqual(set(s), {"status", "disjuntor_llm", "disjuntor_vetorial", "llm_modo"})
        self.assertEqual(s["status"], "ok")
        with mock.patch.object(llm, "API_KEY", ""):
            self.assertEqual(g.saude()["llm_modo"], "simulado")
        with mock.patch.object(llm, "API_KEY", "k"):
            self.assertEqual(g.saude()["llm_modo"], "real")


if __name__ == "__main__":
    unittest.main()
