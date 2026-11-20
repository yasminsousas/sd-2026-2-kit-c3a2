"""
Funcoes comuns aos scripts de teste (ponta a ponta e resiliencia).

Tudo passa pelo GATEWAY, como faria um cliente de verdade: os servicos internos nao sao
publicados pelo docker-compose. So a biblioteca `requests` e necessaria.

Variaveis: GATEWAY_URL (padrao http://localhost:8000), USUARIO / SENHA (padrao admin/admin123),
JAEGER_URL (padrao http://localhost:16686), URL_LLM_FALSO (padrao http://localhost:9000).
"""
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import requests

RAIZ = Path(__file__).resolve().parent.parent
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000").rstrip("/")
USUARIO = os.getenv("USUARIO", "admin")
SENHA = os.getenv("SENHA", "admin123")
JAEGER_URL = os.getenv("JAEGER_URL", "http://localhost:16686").rstrip("/")
URL_LLM_FALSO = os.getenv("URL_LLM_FALSO", "http://localhost:9000").rstrip("/")

_sessao = requests.Session()
_token = {"valor": None}


def falhou(mensagem):
    sys.exit(f"FALHOU: {mensagem}")


def conferir(condicao, mensagem):
    if not condicao:
        falhou(mensagem)
    print(f"   ok - {mensagem}")


def esperar(condicao, descricao, limite=60.0, intervalo=0.5):
    """Repete `condicao` ate ela devolver algo verdadeiro (ou estourar o limite)."""
    fim = time.time() + limite
    while True:
        try:
            valor = condicao()
            if valor:
                return valor
        except requests.RequestException:
            pass
        if time.time() >= fim:
            falhou(f"tempo esgotado ({limite:g}s) esperando {descricao}")
        time.sleep(intervalo)


def login(usuario=USUARIO, senha=SENHA):
    r = _sessao.post(f"{GATEWAY_URL}/login", json={"usuario": usuario, "senha": senha}, timeout=15)
    if r.status_code == 200:
        _token["valor"] = r.json()["access_token"]
    return r


def chamar(metodo, caminho, autenticado=True, timeout=60, **kwargs):
    """
    Requisicao ao gateway com o token. Se o rate limit responder 429, espera o
    Retry-After e tenta de novo: os testes nao devem falhar por excesso de chamadas.
    """
    headers = kwargs.pop("headers", {})
    if autenticado:
        if _token["valor"] is None:
            login()
        headers["Authorization"] = f"Bearer {_token['valor']}"
    for _ in range(5):
        r = _sessao.request(metodo, f"{GATEWAY_URL}{caminho}", headers=headers,
                            timeout=timeout, **kwargs)
        if r.status_code != 429:
            return r
        espera = float(r.headers.get("Retry-After", "5"))
        print(f"   (rate limit atingido: aguardando {espera:g}s)")
        time.sleep(min(espera, 61))
    return r


def esperar_gateway(limite=120):
    esperar(lambda: _sessao.get(f"{GATEWAY_URL}/saude", timeout=5).ok, "o gateway responder /saude", limite)


def saude_detalhada():
    return chamar("get", "/saude/detalhada", timeout=15).json()


def novo_documento(complemento=""):
    """Documento com marcador e origem unicos desta execucao: nao depende do indice."""
    uid = uuid.uuid4().hex[:10]
    marcador = f"marcador{uid}"
    return {"texto": f"Teste {uid}: o protocolo {marcador} usa quorum de escrita{complemento}.",
            "origem": f"teste-{uid}"}, marcador


def indexar(docs):
    return chamar("post", "/indexar", json=docs, timeout=20)


def contar_origem(origem):
    return chamar("get", "/documentos", timeout=20).json().get("origens", {}).get(origem, 0)


def esperar_indexado(origem, limite=60):
    esperar(lambda: contar_origem(origem) >= 1, f"o worker indexar '{origem}'", limite, intervalo=1)


def perguntar(pergunta, limite=120):
    inicio = time.time()
    r = chamar("post", "/perguntar", json={"pergunta": pergunta}, timeout=limite)
    return r, time.time() - inicio


def enfileirar(texto):
    return chamar("post", "/async/predict", json={"texto": texto}, timeout=20)


def resultado(job_id):
    return chamar("get", f"/resultados/{job_id}", timeout=20)


def esperar_job(job_id, limite=60):
    def pronto():
        r = resultado(job_id)
        return r.json() if r.ok and r.json().get("status") in ("concluido", "falha") else None
    return esperar(pronto, f"o worker processar o job {job_id}", limite, intervalo=1)


def servicos_no_trace(trace_id):
    """Servicos que registraram spans neste trace no Jaeger (vazio se o Jaeger nao responder)."""
    try:
        r = requests.get(f"{JAEGER_URL}/api/traces/{trace_id}", timeout=5)
        if not r.ok:
            return set()
        dados = r.json().get("data") or []
        return {p["serviceName"] for t in dados for p in t.get("processes", {}).values()}
    except (requests.RequestException, ValueError):
        return set()


def jaeger_disponivel():
    try:
        return requests.get(f"{JAEGER_URL}/api/services", timeout=3).ok
    except requests.RequestException:
        return False


def docker_compose(*args):
    """Controla os containers (so usado com --docker). Falha com mensagem clara."""
    try:
        subprocess.run(["docker", "compose", *args], cwd=RAIZ, check=True,
                       capture_output=True, text=True, timeout=180)
    except FileNotFoundError:
        falhou("o comando 'docker' nao foi encontrado; --docker exige o Docker Compose")
    except subprocess.CalledProcessError as erro:
        falhou(f"docker compose {' '.join(args)}: {erro.stderr.strip()}")
