"""Testes do API Gateway: autenticacao, limite de taxa, fila assincrona, resiliencia e rastreamento."""
import fnmatch
import json
import logging
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import jwt
import redis
import requests
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import servicos.gateway.app as g
from servicos.gateway import autenticacao, config as cfg
from servicos.gateway.limite import Limitador
from servicos.gateway.resiliencia import ClienteServico

SEGREDO = "s" * 48

# exportador em memoria no provedor global configurado pelo gateway: permite conferir spans
EXPORTADOR = InMemorySpanExporter()
trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(EXPORTADOR))


class FakeRedis:
    """Redis em memoria com os comandos que o gateway usa e injecao de falhas por comando."""

    def __init__(self):
        self.dados, self.listas, self.ttl = {}, {}, {}
        self._falhas = {}

    def falhar(self, comando, vezes=1):
        self._falhas[comando] = vezes

    def _entrada(self, comando):
        if self._falhas.get(comando, 0) > 0:
            self._falhas[comando] -= 1
            raise redis.ConnectionError(f"falha injetada em {comando}")

    # operacoes puras (usadas pelos comandos e pelo pipeline)
    def _set(self, chave, valor, ex=None):
        self.dados[chave] = valor
        if ex is not None:
            self.ttl[chave] = ex
        return True

    def _incr(self, chave):
        self.dados[chave] = str(int(self.dados.get(chave, 0)) + 1)
        return int(self.dados[chave])

    def _expire(self, chave, segundos):
        self.ttl[chave] = segundos
        return True

    def _rpush(self, nome, *valores):
        lista = self.listas.setdefault(nome, [])
        lista.extend(valores)
        return len(lista)

    # comandos
    def get(self, chave):
        self._entrada("get")
        return self.dados.get(chave)

    def set(self, chave, valor, ex=None):
        self._entrada("set")
        return self._set(chave, valor, ex)

    def delete(self, *chaves):
        self._entrada("delete")
        return sum(1 for c in chaves if self.dados.pop(c, None) is not None)

    def rpush(self, nome, *valores):
        self._entrada("rpush")
        return self._rpush(nome, *valores)

    def llen(self, nome):
        self._entrada("llen")
        return len(self.listas.get(nome, []))

    def ping(self):
        self._entrada("ping")
        return True

    def scan_iter(self, match="*", count=None):
        self._entrada("scan")
        return iter([c for c in list(self.dados) if fnmatch.fnmatch(c, match)])

    def pipeline(self, transaction=True):
        return _Pipeline(self)


class _Pipeline:
    """MULTI/EXEC: ou aplica todos os comandos, ou nenhum."""

    def __init__(self, r):
        self._r, self._comandos = r, []

    def set(self, *a, **kw):
        self._comandos.append(lambda: self._r._set(*a, **kw))

    def incr(self, *a):
        self._comandos.append(lambda: self._r._incr(*a))

    def expire(self, *a):
        self._comandos.append(lambda: self._r._expire(*a))

    def rpush(self, *a):
        self._comandos.append(lambda: self._r._rpush(*a))

    def execute(self):
        self._r._entrada("execute")
        return [comando() for comando in self._comandos]


class LogsLigados:
    """tests/__init__.py silencia o log da suite; os testes de log religam so durante o bloco."""

    def __enter__(self):
        self._anterior = logging.root.manager.disable
        logging.disable(logging.NOTSET)

    def __exit__(self, *erro):
        logging.disable(self._anterior)


def resposta_http(codigo, corpo=None):
    r = requests.Response()
    r.status_code = codigo
    r._content = json.dumps(corpo).encode() if corpo is not None else b""
    r.headers["Content-Type"] = "application/json"
    return r


class Base(unittest.TestCase):

    def setUp(self):
        for alvo, valor in (("JWT_SEGREDO", SEGREDO), ("USUARIO_PADRAO", "admin"),
                            ("SENHA_PADRAO", "senha-forte"), ("RATE_LIMIT_RPM", 100),
                            ("LOGIN_RATE_LIMIT_RPM", 100), ("TTL_RESULTADOS_SEG", 3600)):
            self._patch(cfg, alvo, valor)
        self.redis = FakeRedis()
        self._patch(g, "r", self.redis)
        self.relogio = mock.Mock(return_value=6000.0)       # inicio de um minuto
        self._patch(g, "limitador", Limitador(lambda: g.r, espera_retentar=5, relogio=self.relogio))
        self.inferencia = self._cliente("inferencia")
        self.vetorial = self._cliente("vetorial")
        self._patch(g, "cliente_inferencia", self.inferencia)
        self._patch(g, "cliente_vetorial", self.vetorial)
        self.client = TestClient(g.app)
        EXPORTADOR.clear()

    def _patch(self, alvo, nome, valor):
        patcher = mock.patch.object(alvo, nome, valor)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _cliente(self, nome):
        sessao = mock.Mock()
        cliente = ClienteServico(nome, f"http://{nome}", timeout=5, tentativas=3, backoff=0.3,
                                 limite_falhas=3, espera_circuito=30, sessao=sessao,
                                 dormir=mock.Mock(), sortear=lambda a, b: 0.0)
        return cliente

    def token(self, usuario="admin", **extra):
        agora = extra.pop("agora", datetime.now(timezone.utc))
        payload = {"sub": usuario, "iss": "plataforma-ia", "iat": agora,
                   "exp": agora + timedelta(minutes=60)}
        payload.update(extra)
        payload = {k: v for k, v in payload.items() if v is not None}
        return jwt.encode(payload, extra.get("_segredo", SEGREDO), algorithm="HS256")

    def auth(self, usuario="admin"):
        return {"Authorization": f"Bearer {self.token(usuario)}"}


class Configuracao(unittest.TestCase):

    def test_producao_exige_segredo_forte(self):
        for valor in (None, "", "curto", "x" * 31):
            with self.assertRaises(cfg.ConfiguracaoInsegura):
                cfg.carregar_segredo_jwt(valor, "producao")
        self.assertEqual(cfg.carregar_segredo_jwt("x" * 32, "producao"), "x" * 32)

    def test_desenvolvimento_aceita_com_aviso(self):
        with LogsLigados(), self.assertLogs("gateway.config", "WARNING"):
            self.assertEqual(cfg.carregar_segredo_jwt("curto", "desenvolvimento"), "curto")
        with LogsLigados(), self.assertLogs("gateway.config", "WARNING"):
            aleatorio = cfg.carregar_segredo_jwt(None, "desenvolvimento")
        self.assertGreaterEqual(len(aleatorio), 32)

    def test_producao_exige_senha(self):
        with self.assertRaises(cfg.ConfiguracaoInsegura):
            cfg.carregar_senha(None, "producao")
        self.assertEqual(cfg.carregar_senha("abc", "producao"), "abc")


class Login(Base):

    def test_login_ok_devolve_token_valido(self):
        resp = self.client.post("/login", json={"usuario": "admin", "senha": "senha-forte"})
        self.assertEqual(resp.status_code, 200)
        corpo = resp.json()
        self.assertEqual(corpo["token_type"], "bearer")
        self.assertEqual(corpo["expira_em_min"], cfg.JWT_EXPIRACAO_MIN)
        dados = jwt.decode(corpo["access_token"], SEGREDO, algorithms=["HS256"], issuer="plataforma-ia")
        self.assertEqual(dados["sub"], "admin")
        self.assertIn("iat", dados)

    def test_credenciais_erradas_401_generico(self):
        r1 = self.client.post("/login", json={"usuario": "admin", "senha": "errada"})
        r2 = self.client.post("/login", json={"usuario": "outro", "senha": "senha-forte"})
        for resp in (r1, r2):
            self.assertEqual(resp.status_code, 401)
            self.assertIn("WWW-Authenticate", resp.headers)
        self.assertEqual(r1.json(), r2.json())   # nao revela qual campo errou

    def test_login_tem_rate_limit_por_ip(self):
        self._patch(cfg, "LOGIN_RATE_LIMIT_RPM", 3)
        codigos = [self.client.post("/login", json={"usuario": "admin", "senha": "x"}).status_code
                   for _ in range(4)]
        self.assertEqual(codigos, [401, 401, 401, 429])
        # nem a senha certa passa enquanto a janela nao virar
        resp = self.client.post("/login", json={"usuario": "admin", "senha": "senha-forte"})
        self.assertEqual(resp.status_code, 429)
        self.assertIn("Retry-After", resp.headers)
        self.assertTrue(any(c.startswith("ratelimit:login:") for c in self.redis.dados))


class Token(Base):

    def assert401(self, cabecalhos, trecho):
        resp = self.client.post("/predict", json={"texto": "oi"}, headers=cabecalhos)
        self.assertEqual(resp.status_code, 401)
        self.assertIn("Bearer", resp.headers["WWW-Authenticate"])
        self.assertIn(trecho, resp.json()["detail"])

    def test_sem_token(self):
        self.assert401({}, "ausente")
        self.assert401({"Authorization": "Basic YWRtaW46YWRtaW4="}, "ausente")

    def test_expirado(self):
        velho = datetime.now(timezone.utc) - timedelta(hours=3)
        self.assert401({"Authorization": f"Bearer {self.token(agora=velho)}"}, "expirado")

    def test_invalidos(self):
        outro_segredo = jwt.encode({"sub": "admin", "iss": "plataforma-ia",
                                    "iat": datetime.now(timezone.utc),
                                    "exp": datetime.now(timezone.utc) + timedelta(minutes=5)},
                                   "o" * 48, algorithm="HS256")
        sem_alg = jwt.encode({"sub": "admin", "iss": "plataforma-ia"}, None, algorithm="none")
        for token in ("lixo", outro_segredo, sem_alg, self.token(iss="outro-emissor"),
                      self.token(exp=None), self.token(iat=None)):
            self.assert401({"Authorization": f"Bearer {token}"}, "invalido")


class RateLimit(Base):

    def setUp(self):
        super().setUp()
        self._patch(cfg, "RATE_LIMIT_RPM", 2)
        self.inferencia._sessao.request.return_value = resposta_http(200, {"sentimento": "positivo"})

    def pedir(self):
        return self.client.post("/predict", json={"texto": "bom"}, headers=self.auth())

    def test_429_com_retry_after_e_cabecalhos(self):
        r1, r2, r3 = self.pedir(), self.pedir(), self.pedir()
        self.assertEqual((r1.status_code, r2.status_code, r3.status_code), (200, 200, 429))
        self.assertEqual(r1.headers["X-RateLimit-Limit"], "2")
        self.assertEqual(r1.headers["X-RateLimit-Remaining"], "1")
        self.assertEqual(r3.headers["X-RateLimit-Remaining"], "0")
        self.assertEqual(r3.headers["Retry-After"], "60")
        chave = f"ratelimit:{autenticacao.hash_usuario('admin')}:{int(6000 // 60)}"
        self.assertEqual(self.redis.dados[chave], "3")
        self.assertEqual(self.redis.ttl[chave], 65)      # a chave sempre expira
        self.assertNotIn("admin", "".join(self.redis.dados))   # usuario nunca em claro

    def test_janela_vira_e_libera(self):
        self.pedir(), self.pedir()
        self.assertEqual(self.pedir().status_code, 429)
        self.relogio.return_value = 6060.0
        self.assertEqual(self.pedir().status_code, 200)

    def test_redis_fora_cai_para_memoria_sem_derrubar(self):
        self.redis.falhar("execute", 100)
        with LogsLigados(), self.assertLogs("gateway.limite", "WARNING"):
            r1 = self.pedir()
        r2, r3 = self.pedir(), self.pedir()
        self.assertEqual((r1.status_code, r2.status_code, r3.status_code), (200, 200, 429))
        self.assertEqual(g.limitador.disjuntor.estado, "aberto")   # nao insiste no Redis
        self.assertEqual(self.redis._falhas["execute"], 99)


class Assincrono(Base):

    def test_enfileira_mensagem_com_trace_e_dono(self):
        resp = self.client.post("/async/predict", json={"texto": "  adorei  "}, headers=self.auth())
        self.assertEqual(resp.status_code, 202)
        corpo = resp.json()
        job_id = corpo["job_id"]
        self.assertRegex(job_id, r"^[0-9a-f]{32}$")
        self.assertEqual(corpo["status"], "enfileirado")
        self.assertEqual(corpo["consultar_em"], f"/resultados/{job_id}")
        self.assertEqual(corpo["trace_id"], resp.headers["X-Trace-ID"])

        [bruta] = self.redis.listas["fila:inferencia"]
        msg = json.loads(bruta)
        dono = autenticacao.hash_usuario("admin")
        self.assertEqual(msg["job_id"], job_id)
        self.assertEqual(msg["texto"], "adorei")
        self.assertEqual(msg["dono"], dono)
        self.assertIn("criado_em", msg)
        self.assertIn(corpo["trace_id"], msg["_trace"]["traceparent"])

        recibo = json.loads(self.redis.dados[f"resultado:{job_id}"])
        self.assertEqual((recibo["status"], recibo["dono"]), ("enfileirado", dono))
        self.assertEqual(self.redis.ttl[f"resultado:{job_id}"], 3600)

        produtor = [s for s in EXPORTADOR.get_finished_spans()
                    if s.name == "fila.publicar fila:inferencia"]
        self.assertEqual(len(produtor), 1)
        self.assertEqual(produtor[0].kind, trace.SpanKind.PRODUCER)
        self.assertEqual(format(produtor[0].context.trace_id, "032x"), corpo["trace_id"])

    def test_redis_fora_503_sem_meio_job(self):
        # 1a falha: contador do rate limit (cai para memoria); 2a: o pipeline do enfileiramento
        self.redis.falhar("execute", 2)
        resp = self.client.post("/async/predict", json={"texto": "x"}, headers=self.auth())
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(self.redis.listas, {})
        self.assertFalse([c for c in self.redis.dados if c.startswith("resultado:")])


class Resultados(Base):

    def setUp(self):
        super().setUp()
        self.job = "a" * 32
        self.redis.dados[f"resultado:{self.job}"] = json.dumps({
            "job_id": self.job, "status": "concluido", "dono": autenticacao.hash_usuario("admin"),
            "resultado": {"sentimento": "positivo", "confianca": 0.9}})

    def test_dono_le_sem_o_campo_dono(self):
        resp = self.client.get(f"/resultados/{self.job}", headers=self.auth())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "concluido")
        self.assertNotIn("dono", resp.json())

    def test_outro_usuario_recebe_404_igual_inexistente(self):
        outro = self.client.get(f"/resultados/{self.job}", headers=self.auth("mallory"))
        inexistente = self.client.get(f"/resultados/{'b' * 32}", headers=self.auth())
        self.assertEqual((outro.status_code, inexistente.status_code), (404, 404))
        self.assertEqual(outro.json(), inexistente.json())

    def test_job_id_mal_formado_422(self):
        for job in ("abc", "../../etc", "Z" * 32):
            resp = self.client.get(f"/resultados/{job}", headers=self.auth())
            self.assertIn(resp.status_code, (404, 422), job)
        self.assertEqual(self.client.get("/resultados/abc", headers=self.auth()).status_code, 422)
        uuid_canonico = "12345678-1234-1234-1234-123456789abc"
        self.assertEqual(self.client.get(f"/resultados/{uuid_canonico}", headers=self.auth()).status_code, 404)

    def test_delete_apaga_e_respeita_dono(self):
        resp = self.client.delete(f"/resultados/{self.job}", headers=self.auth("mallory"))
        self.assertEqual(resp.status_code, 404)
        self.assertIn(f"resultado:{self.job}", self.redis.dados)
        resp = self.client.delete(f"/resultados/{self.job}", headers=self.auth())
        self.assertEqual(resp.status_code, 204)
        self.assertEqual(resp.content, b"")
        self.assertNotIn(f"resultado:{self.job}", self.redis.dados)
        self.assertEqual(self.client.get(f"/resultados/{self.job}", headers=self.auth()).status_code, 404)

    def test_redis_fora_503(self):
        self.redis.falhar("get")
        self.assertEqual(self.client.get(f"/resultados/{self.job}", headers=self.auth()).status_code, 503)


class Resiliencia(Base):

    def predict(self):
        return self.client.post("/predict", json={"texto": "bom"}, headers=self.auth())

    def test_retentativa_em_erro_transitorio(self):
        sessao = self.inferencia._sessao
        sessao.request.side_effect = [requests.ConnectionError("http://inferencia"),
                                      resposta_http(502),
                                      resposta_http(200, {"sentimento": "positivo"})]
        resp = self.predict()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"sentimento": "positivo"})
        self.assertEqual(sessao.request.call_count, 3)
        self.assertEqual([c.args[0] for c in self.inferencia._dormir.call_args_list], [0.3, 0.6])
        self.assertEqual(self.inferencia.disjuntor.falhas, 0)   # o sucesso zera

    def test_backoff_tem_jitter(self):
        cliente = ClienteServico("x", "http://x", backoff=0.3, sortear=lambda a, b: b)
        self.assertAlmostEqual(cliente.espera(1), 0.6)
        self.assertAlmostEqual(cliente.espera(3), 1.5)

    def test_circuito_abre_e_responde_503_na_hora(self):
        sessao = self.inferencia._sessao
        sessao.request.side_effect = requests.Timeout("http://inferencia")
        resp = self.predict()      # 3 tentativas = limite de 3 falhas -> abre
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(self.inferencia.disjuntor.estado, "aberto")
        self.assertNotIn("http://", resp.json()["detail"])      # nao vaza URL interna
        chamadas = sessao.request.call_count
        resp = self.predict()
        self.assertEqual(resp.status_code, 503)
        self.assertIn("circuito aberto", resp.json()["detail"])
        self.assertEqual(resp.headers["Retry-After"], "30")
        self.assertEqual(sessao.request.call_count, chamadas)   # nem tentou
        # o vetorial tem disjuntor proprio e segue fechado
        self.assertEqual(self.vetorial.disjuntor.estado, "fechado")

    def test_503_degradado_repassado_sem_retentar(self):
        degradado = {"erro": "o servico de linguagem nao respondeu", "degradado": True,
                     "contexto": ["trecho"], "fontes": ["a.txt"]}
        sessao = self.inferencia._sessao
        sessao.request.return_value = resposta_http(503, degradado)
        for _ in range(5):
            resp = self.client.post("/perguntar", json={"pergunta": "o que?"}, headers=self.auth())
            self.assertEqual(resp.status_code, 503)
            self.assertEqual(resp.json(), degradado)
        self.assertEqual(sessao.request.call_count, 5)
        self.assertEqual(self.inferencia.disjuntor.estado, "fechado")
        self.assertEqual(self.inferencia.disjuntor.falhas, 0)
        self.assertEqual(sessao.request.call_args.kwargs["timeout"], cfg.GATEWAY_TIMEOUT_PERGUNTAR)
        self.assertEqual(sessao.request.call_args.kwargs["json"], {"pergunta": "o que?", "top_k": 3})

    def test_503_sem_degradado_e_falha_transitoria(self):
        sessao = self.inferencia._sessao
        sessao.request.side_effect = [resposta_http(503, {"erro": "x"}),
                                      resposta_http(200, {"sentimento": "neutro"})]
        self.assertEqual(self.predict().status_code, 200)
        self.assertEqual(sessao.request.call_count, 2)

    def test_4xx_repassado_sem_retentar_nem_abrir(self):
        sessao = self.inferencia._sessao
        sessao.request.return_value = resposta_http(422, {"detail": "texto invalido"})
        for _ in range(5):
            resp = self.predict()
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json(), {"detail": "texto invalido"})
        self.assertEqual(sessao.request.call_count, 5)
        self.assertEqual(self.inferencia.disjuntor.estado, "fechado")

    def test_indexar_nao_retenta_apos_timeout_de_leitura(self):
        sessao = self.vetorial._sessao
        sessao.request.side_effect = requests.ReadTimeout()
        docs = [{"texto": "conteudo", "origem": "a.txt"}]
        self.assertEqual(self.client.post("/indexar", json=docs, headers=self.auth()).status_code, 503)
        self.assertEqual(sessao.request.call_count, 1)
        # falha de conexao: o pedido nem chegou, pode repetir
        sessao.request.reset_mock()
        sessao.request.side_effect = [requests.ConnectionError(),
                                      resposta_http(202, {"enfileirados": 1, "na_fila": 1})]
        resp = self.client.post("/indexar", json=docs, headers=self.auth())
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.json(), {"enfileirados": 1, "na_fila": 1})
        self.assertEqual(sessao.request.call_args.args[:2], ("POST", "http://vetorial/indexar"))

    def test_documentos_repassa(self):
        self.vetorial._sessao.request.return_value = resposta_http(200, {"total": 2, "origens": {"a": 2}})
        resp = self.client.get("/documentos", headers=self.auth())
        self.assertEqual(resp.json(), {"total": 2, "origens": {"a": 2}})
        self.assertEqual(self.vetorial._sessao.request.call_args.args, ("GET", "http://vetorial/documentos"))


class Validacao(Base):

    def test_limites(self):
        casos = [("/predict", {"texto": ""}), ("/predict", {"texto": "   "}),
                 ("/predict", {"texto": "x" * 5001}), ("/async/predict", {}),
                 ("/perguntar", {"pergunta": "x" * 2001}), ("/perguntar", {"pergunta": "p", "top_k": 0}),
                 ("/indexar", []), ("/indexar", [{"texto": "t", "origem": "o"}] * 51),
                 ("/indexar", [{"texto": "t"}])]
        for rota, corpo in casos:
            resp = self.client.post(rota, json=corpo, headers=self.auth())
            self.assertEqual(resp.status_code, 422, (rota, str(corpo)[:40]))
        self.assertEqual(self.inferencia._sessao.request.call_count, 0)
        self.assertEqual(self.vetorial._sessao.request.call_count, 0)

    def test_corpo_gigante_413(self):
        self._patch(cfg, "GATEWAY_MAX_CORPO_BYTES", 100)
        resp = self.client.post("/predict", json={"texto": "x" * 200}, headers=self.auth())
        self.assertEqual(resp.status_code, 413)


class RotasPublicasERastreamento(Base):

    def test_raiz_e_saude_sem_token(self):
        raiz = self.client.get("/").json()
        self.assertEqual(raiz["nome"], "api-gateway")
        self.assertIn("POST /async/predict", raiz["rotas"])
        resp = self.client.get("/saude")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")
        self.assertRegex(resp.headers["X-Trace-ID"], r"^[0-9a-f]{32}$")

    def test_saude_detalhada(self):
        self.assertEqual(self.client.get("/saude/detalhada").status_code, 401)
        self.redis.dados["worker:batimento:w1"] = "1"
        self.redis.dados["worker:batimento:w2"] = "1"
        self.redis.listas["fila:ingestao"] = ["a", "b", "c"]
        self.inferencia._sessao.get.return_value = resposta_http(200, {"status": "ok"})
        self.vetorial._sessao.get.side_effect = requests.ConnectionError("http://vetorial")
        corpo = self.client.get("/saude/detalhada", headers=self.auth()).json()
        self.assertEqual(corpo["status"], "degradado")
        self.assertEqual(corpo["redis"]["workers_vivos"], 2)
        self.assertEqual(corpo["redis"]["filas"], {"fila:inferencia": 0, "fila:ingestao": 3})
        self.assertEqual(corpo["servicos"]["inferencia"]["status"], "ok")
        self.assertEqual(corpo["servicos"]["vetorial"], {"status": "indisponivel", "erro": "falha de conexao"})
        self.assertEqual(corpo["disjuntores"]["inferencia"], "fechado")
        self.assertEqual(self.vetorial.disjuntor.falhas, 0)   # diagnostico nao conta falha
        self.assertEqual(self.vetorial._sessao.get.call_args.kwargs["timeout"], cfg.GATEWAY_TIMEOUT_SAUDE)

    def test_x_trace_id_e_o_trace_do_span_da_requisicao(self):
        self.inferencia._sessao.request.return_value = resposta_http(200, {"sentimento": "positivo"})
        resp = self.client.post("/predict", json={"texto": "bom"}, headers=self.auth())
        trace_id = resp.headers["X-Trace-ID"]
        servidor = [s for s in EXPORTADOR.get_finished_spans() if s.kind == trace.SpanKind.SERVER]
        self.assertTrue(servidor)
        self.assertEqual(format(servidor[-1].context.trace_id, "032x"), trace_id)

    def test_cabecalhos_de_seguranca_e_hsts(self):
        resp = self.client.get("/")
        self.assertEqual(resp.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resp.headers["X-Frame-Options"], "DENY")
        self.assertIn("Referrer-Policy", resp.headers)
        self.assertNotIn("Strict-Transport-Security", resp.headers)
        resp = self.client.get("/", headers={"X-Forwarded-Proto": "https"})
        self.assertIn("max-age=", resp.headers["Strict-Transport-Security"])

    def test_log_uma_linha_sem_dados_pessoais(self):
        self.inferencia._sessao.request.return_value = resposta_http(200, {"sentimento": "positivo"})
        token = self.token()
        with LogsLigados(), self.assertLogs("gateway", "INFO") as logs:
            self.client.post("/predict", json={"texto": "TEXTO-SECRETO"},
                             headers={"Authorization": f"Bearer {token}"})
            self.client.post("/login", json={"usuario": "admin", "senha": "SENHA-ERRADA"})
        saida = "\n".join(logs.output)
        for proibido in ("TEXTO-SECRETO", "SENHA-ERRADA", token, "admin"):
            self.assertNotIn(proibido, saida)
        linhas = [l for l in logs.output if "requisicao metodo=POST rota=/predict" in l]
        self.assertEqual(len(linhas), 1)
        self.assertIn("status=200", linhas[0])
        self.assertIn("trace_id=", linhas[0])
        self.assertIn("latencia_ms=", linhas[0])


if __name__ == "__main__":
    unittest.main()
