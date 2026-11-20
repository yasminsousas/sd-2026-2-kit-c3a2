"""
Servico vetorial: POST /buscar (porte dos testes de recuperacao do C2.A2, agora lendo o
indice direto do disco), GET /documentos e GET /saude.
"""
import unittest
from unittest import mock

import numpy as np
from fastapi import HTTPException
from sklearn.feature_extraction.text import TfidfVectorizer

import servicos.vetorial.app as vet
from servicos.comum import indice
from servicos.comum.embeddings import Vetorizador, similaridade
from tests.util import FakeRedis, remendar, usar_indice_temporario

DOCS = [
    {"texto": "gato preto dorme no sofa", "origem": "a"},
    {"texto": "cachorro marrom corre no parque", "origem": "b"},
    {"texto": "gato e cachorro brincam no parque", "origem": "c"},
    {"texto": "a bolsa de valores fechou em alta", "origem": "d"},
]


def buscar(pergunta, top_k=3):
    return vet.buscar(vet.Consulta(pergunta=pergunta, top_k=top_k))["resultados"]


class Cosseno(unittest.TestCase):

    def test_casos_basicos(self):
        self.assertAlmostEqual(similaridade([1, 2, 3], [1, 2, 3]), 1.0)
        self.assertAlmostEqual(similaridade([1, 0], [0, 1]), 0.0)
        self.assertAlmostEqual(similaridade([1, 2], [-1, -2]), -1.0)
        self.assertEqual(similaridade([0, 0], [1, 2]), 0.0)          # vetor nulo
        self.assertAlmostEqual(similaridade([1, 2, 3], [2, 4, 6]), 1.0)   # ignora a escala


class Busca(unittest.TestCase):

    def setUp(self):
        usar_indice_temporario(self)
        indice.adicionar(DOCS)
        self.itens = indice.carregar()

    def esperado(self, pergunta):
        """Calculo independente, direto no scikit-learn, do ranking esperado."""
        tfidf = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
        matriz = tfidf.fit_transform([d["texto"] for d in DOCS]).toarray()
        q = tfidf.transform([pergunta]).toarray()[0]
        scores = matriz @ q / (np.linalg.norm(matriz, axis=1) * np.linalg.norm(q))
        ordem = sorted(range(len(DOCS)), key=lambda i: -scores[i])
        return [(DOCS[i]["origem"], round(float(scores[i]), 4)) for i in ordem if scores[i] > 0]

    def test_vetores_do_indice_e_da_pergunta_usam_o_mesmo_vocabulario(self):
        v = Vetorizador()
        v.ajustar([i["texto"] for i in self.itens])
        for item in self.itens:
            self.assertEqual(v.vetorizar(item["texto"]), item["vetor"])
        self.assertEqual(len({len(i["vetor"]) for i in self.itens}), 1)

    def test_ranking_e_scores_batem_com_calculo_independente(self):
        for pergunta in ("gato", "cachorro no parque", "bolsa de valores", "gato preto sofa"):
            obtido = [(r["origem"], r["score"]) for r in buscar(pergunta, top_k=10)]
            self.assertEqual(obtido, self.esperado(pergunta), pergunta)

    def test_top_k_limita_e_mantem_a_ordem_decrescente(self):
        todos = buscar("gato cachorro parque no", top_k=10)
        self.assertGreater(len(todos), 2)
        self.assertEqual(buscar("gato cachorro parque no", top_k=2), todos[:2])
        scores = [r["score"] for r in todos]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_resultado_e_deterministico_em_empate(self):
        iguais = [{"id": o, "texto": "gato", "origem": o, "vetor": self.itens[0]["vetor"]}
                  for o in "xyz"]
        indice._salvar(iguais + [self.itens[1]])
        self.assertEqual([r["origem"] for r in buscar("gato")], ["x", "y", "z"])

    def test_sem_correspondencia_lexical_devolve_vazio(self):
        for pergunta in ("zzxqw", "   ", "!!!", "a"):
            self.assertEqual(buscar(pergunta), [], repr(pergunta))

    def test_documentos_sem_termo_em_comum_ficam_de_fora_mesmo_com_top_k_grande(self):
        self.assertEqual([r["origem"] for r in buscar("bolsa", top_k=10)], ["d"])

    def test_encontra_documento_recem_indexado_sem_reiniciar(self):
        indice.adicionar([{"texto": "o dragao roxo voa alto", "origem": "novo"}])
        self.assertEqual(buscar("dragao")[0]["origem"], "novo")

    def test_resultado_nao_traz_vetores(self):
        self.assertEqual(set(buscar("gato")[0]), {"texto", "origem", "score"})


class IndiceProblematico(unittest.TestCase):

    def setUp(self):
        self.base = usar_indice_temporario(self)

    def gravar(self, conteudo):
        (self.base / "indice.json").write_text(conteudo, encoding="utf-8")

    def assertIndisponivel(self, pergunta="gato"):
        with self.assertRaises(HTTPException) as ctx:
            buscar(pergunta)
        self.assertEqual(ctx.exception.status_code, 503)

    def test_indice_inexistente_ou_vazio(self):
        self.assertEqual(buscar("gato"), [])
        indice._salvar([])
        self.assertEqual(buscar("gato"), [])

    def test_arquivo_corrompido_responde_503(self):
        for conteudo in ("", "{nao e json", "[1, 2"):
            self.gravar(conteudo)
            self.assertIndisponivel()

    def test_itens_com_formato_invalido(self):
        for itens in ({"itens": []}, "lixo", ["lixo"], [{"texto": "gato"}],
                      [{"texto": "gato", "origem": "a", "vetor": "x"}],
                      [{"texto": 1, "origem": "a", "vetor": [1.0]}]):
            indice._salvar(itens)
            self.assertIndisponivel()

    def test_vetor_com_valores_invalidos(self):
        indice._salvar([{"texto": "gato preto", "origem": "a", "vetor": ["a", "b", "c"]}])
        self.assertIndisponivel()

    def test_vetor_de_dimensao_diferente_e_recalculado(self):
        indice._salvar([{"texto": "gato preto", "origem": "a", "vetor": [1.0]}])
        r = buscar("gato")
        self.assertEqual([x["origem"] for x in r], ["a"])
        self.assertGreater(r[0]["score"], 0)

    def test_indice_so_com_textos_sem_palavras(self):
        indice._salvar([{"texto": "!!!", "origem": "lixo", "vetor": [0.0]}])
        self.assertEqual(buscar("gato"), [])

    def test_arquivo_ilegivel_responde_503(self):
        with mock.patch.object(indice, "carregar", side_effect=PermissionError()):
            self.assertIndisponivel()


class DocumentosESaude(unittest.TestCase):

    def setUp(self):
        self.base = usar_indice_temporario(self)
        self.r = FakeRedis()
        remendar(self, vet, _redis=self.r, FILA="fila:teste")

    def test_documentos_conta_por_origem_sem_textos(self):
        self.assertEqual(vet.documentos(), {"total": 0, "origens": {}})
        indice.adicionar(DOCS + [{"texto": "outro texto do a", "origem": "a"}])
        r = vet.documentos()
        self.assertEqual(r, {"total": 5, "origens": {"a": 2, "b": 1, "c": 1, "d": 1}})
        self.assertNotIn("gato", str(r))

    def test_documentos_com_indice_corrompido_responde_503(self):
        (self.base / "indice.json").write_text("lixo", encoding="utf-8")
        with self.assertRaises(HTTPException) as ctx:
            vet.documentos()
        self.assertEqual(ctx.exception.status_code, 503)

    def test_saude_no_formato_do_contrato(self):
        indice.adicionar(DOCS)
        self.r.rpush("fila:teste", "x", "y")
        self.assertEqual(vet.saude(), {"status": "ok", "documentos": 4, "fila": "ok",
                                       "pendentes_na_fila": 2})

    def test_saude_sobrevive_a_indice_corrompido_e_redis_fora(self):
        (self.base / "indice.json").write_text("lixo", encoding="utf-8")
        self.r.falhar("llen")
        self.assertEqual(vet.saude(), {"status": "ok", "documentos": None,
                                       "fila": "indisponivel", "pendentes_na_fila": None})


class ViaHttp(unittest.TestCase):
    """As rotas respondem pelo FastAPI de verdade (validacao do pydantic e codigos HTTP)."""

    def setUp(self):
        from fastapi.testclient import TestClient
        usar_indice_temporario(self)
        indice.adicionar(DOCS)
        remendar(self, vet, _redis=FakeRedis(), FILA="fila:teste")
        self.cliente = TestClient(vet.app)

    def test_buscar_documentos_saude_e_indexar(self):
        r = self.cliente.post("/buscar", json={"pergunta": "bolsa", "top_k": 1})
        self.assertEqual((r.status_code, r.json()["resultados"][0]["origem"]), (200, "d"))
        self.assertEqual(self.cliente.get("/documentos").json()["total"], 4)
        self.assertEqual(self.cliente.get("/saude").status_code, 200)
        r = self.cliente.post("/indexar", json=[{"texto": "novo texto", "origem": "n"}])
        self.assertEqual((r.status_code, r.json()), (202, {"enfileirados": 1, "na_fila": 1}))

    def test_entradas_invalidas_sao_422(self):
        for rota, corpo in (("/buscar", {"pergunta": ""}), ("/buscar", {"pergunta": "x", "top_k": 0}),
                            ("/indexar", []), ("/indexar", [{"texto": "!!"}])):
            self.assertEqual(self.cliente.post(rota, json=corpo).status_code, 422, (rota, corpo))


if __name__ == "__main__":
    unittest.main()
