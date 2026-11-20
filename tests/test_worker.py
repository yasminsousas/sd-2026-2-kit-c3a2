"""
Processo do worker (servicos/worker/worker.py): semeadura do indice, batimento, laco
robusto dos consumidores e selecao de filas por WORKER_FILAS.
"""
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import redis

from servicos.comum import indice
from servicos.worker import worker as wk
from servicos.worker.consumidor_inferencia import Pausar
from tests.util import FakeRedis, remendar, usar_indice_temporario


class Semeadura(unittest.TestCase):

    def setUp(self):
        usar_indice_temporario(self)
        pasta = tempfile.TemporaryDirectory()
        self.addCleanup(pasta.cleanup)
        self.pasta = Path(pasta.name)
        (self.pasta / "teorema_cap.txt").write_text("O teorema CAP diz...", encoding="utf-8")
        (self.pasta / "raft.txt").write_text("Consenso com lider", encoding="utf-8")
        (self.pasta / "vazio.txt").write_text("!!!", encoding="utf-8")
        (self.pasta / "nota.md").write_text("ignorado por nao ser txt", encoding="utf-8")

    def test_indice_vazio_recebe_os_txt_com_origem_pelo_nome(self):
        self.assertEqual(wk.semear(str(self.pasta)), 2)
        self.assertEqual(sorted(i["origem"] for i in indice.carregar()), ["raft", "teorema_cap"])

    def test_main_semeia_quando_roda_a_ingestao(self):
        remendar(self, wk, SEMEAR_DOCUMENTOS=str(self.pasta), FILAS=["ingestao"])
        parado = threading.Event()
        parado.set()
        with mock.patch.object(wk, "conectar", return_value=FakeRedis()), \
                mock.patch.object(wk.signal, "signal"), \
                mock.patch.object(wk.threading, "Thread"), \
                mock.patch.object(wk.threading, "Event", return_value=parado), \
                mock.patch.object(wk, "encerrar_rastreamento"):
            self.assertEqual(wk.main(), 0)
        self.assertEqual(len(indice.carregar()), 2)

    def test_indice_ja_preenchido_nao_e_semeado(self):
        indice.adicionar([{"texto": "documento existente", "origem": "x"}])
        self.assertEqual(wk.semear(str(self.pasta)), 0)
        self.assertEqual([i["origem"] for i in indice.carregar()], ["x"])

    def test_documentos_do_repositorio_sao_indexaveis(self):
        raiz = Path(__file__).resolve().parent.parent / "documentos"
        self.assertGreater(wk.semear(str(raiz)), 0)

    def test_main_so_semeia_se_roda_a_ingestao(self):
        remendar(self, wk, SEMEAR_DOCUMENTOS=str(self.pasta), FILAS=["inferencia"])
        parado = threading.Event()
        parado.set()                              # main() volta logo depois de iniciar
        with mock.patch.object(wk, "semear") as semear, \
                mock.patch.object(wk, "conectar", return_value=FakeRedis()), \
                mock.patch.object(wk.signal, "signal"), \
                mock.patch.object(wk.threading, "Thread") as thread, \
                mock.patch.object(wk.threading, "Event", return_value=parado), \
                mock.patch.object(wk, "encerrar_rastreamento"):
            self.assertEqual(wk.main(), 0)
        semear.assert_not_called()
        nomes = [c.kwargs["name"] for c in thread.call_args_list]
        self.assertEqual(nomes, ["consumidor-inferencia", "batimento"])   # batimento sempre


class Batimento(unittest.TestCase):

    def test_bater_grava_a_chave_do_host_com_ttl_15(self):
        r = FakeRedis()
        wk.bater(r, ["ingestao"])
        chave = f"worker:batimento:{wk.HOSTNAME}"
        self.assertEqual(r.ttl(chave), 15)
        self.assertEqual(json.loads(r.get(chave))["filas"], ["ingestao"])

    def test_batimento_vale_para_qualquer_selecao_de_filas(self):
        for filas in (["ingestao"], ["inferencia"], ["inferencia", "ingestao"]):
            r, parar = FakeRedis(), threading.Event()
            vivo = mock.Mock(is_alive=mock.Mock(return_value=True))
            with mock.patch.object(parar, "wait", side_effect=lambda *_: parar.set()):
                wk.executar_batimento(r, filas, [vivo] * len(filas), parar)
            self.assertEqual(r.ttl(wk.chave_batimento()), 15, filas)

    def test_consumidor_morto_suspende_o_batimento(self):
        r, parar = FakeRedis(), threading.Event()
        morto = mock.Mock(is_alive=mock.Mock(return_value=False))
        with mock.patch.object(parar, "wait", side_effect=lambda *_: parar.set()):
            wk.executar_batimento(r, ["inferencia"], [morto], parar)
        self.assertIsNone(r.get(wk.chave_batimento()))

    def test_redis_fora_nao_derruba_o_batimento(self):
        r, parar = FakeRedis(), threading.Event()
        r.falhar("setex", 1)
        voltas = []

        def esperar(*_):
            voltas.append(1)
            if len(voltas) == 2:
                parar.set()

        with mock.patch.object(parar, "wait", side_effect=esperar):
            wk.executar_batimento(r, ["ingestao"], [], parar)
        self.assertEqual(r.ttl(wk.chave_batimento()), 15)   # a segunda volta conseguiu


class LacoDoConsumidor(unittest.TestCase):

    def rodar(self, efeitos):
        """Roda o laco com um worker falso; para quando os efeitos acabam."""
        parar = threading.Event()
        efeitos = list(efeitos)

        def passo():
            if not efeitos:
                parar.set()
                return 0
            efeito = efeitos.pop(0)
            if isinstance(efeito, BaseException):
                raise efeito
            return efeito

        esperas = []
        with mock.patch.object(parar, "wait", side_effect=lambda s: esperas.append(s)):
            wk.executar_consumidor("teste", mock.Mock(passo=passo), parar)
        return esperas

    def test_erros_do_redis_e_inesperados_nao_encerram_o_laco(self):
        esperas = self.rodar([redis.ConnectionError(), RuntimeError("bug"), 1, 0])
        self.assertEqual(esperas, [wk.ESPERA_ERRO, wk.ESPERA_ERRO])

    def test_circuito_aberto_pausa_sem_girar(self):
        self.assertEqual(self.rodar([Pausar(15), 1]), [15])

    def test_evento_de_parada_encerra(self):
        parar = threading.Event()
        parar.set()
        worker = mock.Mock()
        wk.executar_consumidor("teste", worker, parar)
        worker.passo.assert_not_called()


class Selecao(unittest.TestCase):

    def test_filas_desconhecidas_sao_recusadas(self):
        for filas in (["xpto"], [], ["ingestao", "outra"]):
            with mock.patch.object(wk, "FILAS", filas):
                self.assertEqual(wk.main(), 2)

    def test_cada_fila_tem_o_seu_consumidor(self):
        self.assertEqual(set(wk.CONSUMIDORES), {"ingestao", "inferencia"})
        for modulo in wk.CONSUMIDORES.values():
            self.assertTrue(hasattr(modulo.Worker(FakeRedis()), "passo"))


if __name__ == "__main__":
    unittest.main()
