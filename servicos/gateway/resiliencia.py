"""
Cliente HTTP resiliente para os servicos internos (inferencia e vetorial).

Camadas, de fora para dentro:
  RETENTATIVA com backoff exponencial + jitter  ->  DISJUNTOR (um por servico)  ->  TEMPO-LIMITE

  - cada tentativa passa pelo disjuntor e conta como falha: o circuito abre depressa e,
    aberto, as tentativas restantes sao recusadas na hora (CircuitoAberto nao e retentado);
  - o jitter (aleatorio) evita que varias replicas do gateway retentem todas no mesmo
    instante e martelem juntas o servico que acabou de voltar;
  - so erros TRANSITORIOS sao retentados e abrem o circuito: falha de conexao, tempo-limite,
    5xx e resposta 2xx com JSON quebrado. Um 4xx prova que o servico esta de pe e que o
    problema e o pedido: e repassado ao cliente sem retentar e sem contar como falha;
  - 503 com {"degradado": true} no corpo e a resposta degradada INTENCIONAL da inferencia
    (o LLM caiu, mas ela devolve o contexto recuperado). O servico respondeu do jeito que
    foi projetado: repassamos ao cliente, sem retentar e contando como sucesso;
  - chamada NAO idempotente (POST /indexar enfileira documentos) nao e retentada apos
    tempo-limite de leitura: o pedido pode ter sido processado e a retentativa duplicaria
    os documentos. Falha de CONEXAO e segura de retentar (o pedido nem chegou).
"""
import logging
import random
import time

import requests

from servicos.comum.circuit_breaker import CircuitBreaker, CircuitoAberto

log = logging.getLogger("gateway.resiliencia")


class ServicoIndisponivel(Exception):
    """O servico interno nao respondeu (ou o circuito esta aberto). Mensagem segura para o cliente."""

    def __init__(self, mensagem: str, circuito_aberto: bool = False):
        super().__init__(mensagem)
        self.circuito_aberto = circuito_aberto


class RespostaInvalida(Exception):
    """Resposta 2xx sem JSON valido: tratada como falha transitoria do servico."""


def erro_transitorio(erro: Exception) -> bool:
    """
    True se vale tentar de novo e se o erro indica indisponibilidade do servico.
    Mesma ideia de servicos.comum.llm.erro_transitorio, mas aqui o 4xx nem vira excecao
    (e repassado), entao so sobram 5xx, rede e resposta quebrada.
    """
    if isinstance(erro, requests.HTTPError):
        return erro.response is None or erro.response.status_code >= 500
    if isinstance(erro, (requests.exceptions.InvalidURL, requests.exceptions.MissingSchema,
                         requests.exceptions.InvalidSchema)):
        return False                        # configuracao errada: repetir nao ajuda
    return isinstance(erro, (requests.RequestException, RespostaInvalida))


def descrever_erro(erro: Exception) -> str:
    """Descricao curta, sem URL nem corpo (str(erro) do requests traz a URL completa)."""
    if isinstance(erro, requests.HTTPError) and erro.response is not None:
        return f"HTTP {erro.response.status_code}"
    if isinstance(erro, requests.Timeout):
        return "tempo-limite excedido"
    if isinstance(erro, requests.ConnectionError):
        return "falha de conexao"
    if isinstance(erro, RespostaInvalida):
        return "resposta invalida"
    return type(erro).__name__


def resposta_degradada(resposta: requests.Response) -> bool:
    if resposta.status_code != 503:
        return False
    try:
        corpo = resposta.json()
    except ValueError:
        return False
    return isinstance(corpo, dict) and bool(corpo.get("degradado"))


class Resposta:
    """O que o gateway repassa ao cliente: status e corpo JSON do servico interno."""

    def __init__(self, status: int, corpo):
        self.status = status
        self.corpo = corpo


class ClienteServico:

    def __init__(self, nome, url_base, timeout=5.0, tentativas=3, backoff=0.3,
                 limite_falhas=5, espera_circuito=30.0, sessao=None, dormir=time.sleep,
                 sortear=random.uniform):
        self.nome = nome
        self.url_base = url_base.rstrip("/")
        self.timeout = timeout
        self.tentativas = max(1, tentativas)
        self.backoff = backoff
        self.disjuntor = CircuitBreaker(limite_falhas=limite_falhas,
                                        espera_segundos=espera_circuito,
                                        conta_como_falha=erro_transitorio)
        self._sessao = sessao or requests.Session()   # reaproveita conexoes (keep-alive)
        self._dormir = dormir
        self._sortear = sortear

    def _uma_chamada(self, metodo, caminho, corpo, timeout) -> Resposta:
        r = self._sessao.request(metodo, self.url_base + caminho, json=corpo, timeout=timeout)
        if r.status_code >= 500 and not resposta_degradada(r):
            raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
        try:
            dados = r.json() if r.content else None
        except ValueError:
            if r.status_code < 300:
                raise RespostaInvalida()
            dados = {"erro": f"erro {r.status_code} do servico {self.nome}"}
        return Resposta(r.status_code, dados)

    def espera(self, tentativa: int) -> float:
        """Backoff exponencial (b, 2b, 4b...) + jitter aleatorio de ate b."""
        return self.backoff * 2 ** (tentativa - 1) + self._sortear(0, self.backoff)

    def chamar(self, metodo, caminho, corpo=None, timeout=None, idempotente=True) -> Resposta:
        timeout = timeout or self.timeout
        ultimo_erro = None
        for tentativa in range(1, self.tentativas + 1):
            try:
                return self.disjuntor.chamar(self._uma_chamada, metodo, caminho, corpo, timeout)
            except CircuitoAberto:
                raise ServicoIndisponivel(
                    f"servico {self.nome} instavel: circuito aberto, nova tentativa em ate "
                    f"{self.disjuntor.espera_segundos:g}s", circuito_aberto=True)
            except Exception as erro:
                ultimo_erro = erro
                repetir = erro_transitorio(erro)
                if not idempotente and isinstance(erro, requests.Timeout) \
                        and not isinstance(erro, requests.ConnectTimeout):
                    repetir = False         # pode ter sido processado: nao duplicar
                log.warning("%s %s falhou (tentativa %d/%d): %s%s", self.nome, caminho,
                            tentativa, self.tentativas, descrever_erro(erro),
                            "" if repetir else " [nao sera repetido]")
                if not repetir or tentativa == self.tentativas:
                    break
                self._dormir(self.espera(tentativa))
        raise ServicoIndisponivel(
            f"servico {self.nome} indisponivel: {descrever_erro(ultimo_erro)}") from ultimo_erro

    def saude(self, timeout: float) -> dict:
        """GET /saude direto, SEM disjuntor: diagnostico nao deve abrir nem fechar o circuito."""
        try:
            r = self._sessao.get(self.url_base + "/saude", timeout=timeout)
            r.raise_for_status()
            return {"status": "ok", "detalhes": r.json()}
        except Exception as erro:
            return {"status": "indisponivel", "erro": descrever_erro(erro)}
