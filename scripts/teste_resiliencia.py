"""
Resiliencia de verdade, pelo gateway: tempo-limite, retentativa, circuit breaker e recuperacao.

Cenarios do LLM (sempre executados) precisam do perfil "demo" do compose, em que a inferencia
aponta para um LLM falso (scripts/llm_falso.py) que conta as chamadas recebidas:

    docker compose --env-file .env.demo --profile demo up --build -d --wait
    python scripts/teste_resiliencia.py --docker

Cenarios de infraestrutura (so com --docker) derrubam e religam containers com
`docker compose stop/start`: inferencia, worker e Redis.

Espera por CONDICOES reais (estado do circuito, job concluido, resposta 200), nao por tempo fixo.
"""
import os
import sys
import time

import apoio
import requests
from apoio import conferir, esperar, perguntar

ESPERA_CIRCUITO = float(os.getenv("LLM_ESPERA_CIRCUITO", "6"))
TIMEOUT_LLM = float(os.getenv("LLM_TIMEOUT", "3"))
TENTATIVAS = int(os.getenv("LLM_TENTATIVAS", "3"))
LIMITE_FALHAS = int(os.getenv("LLM_LIMITE_FALHAS", "3"))


def llm(caminho, metodo="post"):
    r = getattr(requests, metodo)(f"{apoio.URL_LLM_FALSO}{caminho}", timeout=5)
    r.raise_for_status()
    return r.json()


def chamadas_ao_llm():
    return llm("/contagem", "get")["chamadas"]


def disjuntor_llm():
    return apoio.saude_detalhada()["servicos"]["inferencia"]["detalhes"]["disjuntor_llm"]


def disjuntor_gateway(servico):
    return apoio.saude_detalhada()["disjuntores"][servico]


def pergunta_do_teste(marcador):
    return f"o que diz o protocolo {marcador}?"


def fechar_circuito_llm(marcador):
    llm("/modo/ok")
    inicio = time.time()
    esperar(lambda: perguntar(pergunta_do_teste(marcador))[0].status_code == 200,
            "o circuito do LLM fechar apos ele voltar", ESPERA_CIRCUITO + 30, intervalo=0.5)
    conferir(disjuntor_llm() == "fechado",
             f"circuito do LLM fechou {time.time() - inicio:.1f}s depois (espera={ESPERA_CIRCUITO:g}s)")


def cenarios_llm(marcador):
    print("1. LLM saudavel")
    llm("/modo/ok")
    llm("/reset")
    r, _ = perguntar(pergunta_do_teste(marcador))
    conferir(r.status_code == 200 and disjuntor_llm() == "fechado", "resposta 200 e circuito fechado")
    conferir(chamadas_ao_llm() == 1, "exatamente 1 chamada ao LLM")

    print("2. Erro PERMANENTE (HTTP 401): sem retentativa, sem abrir o circuito")
    llm("/modo/401")
    llm("/reset")
    n = LIMITE_FALHAS + 2
    for _ in range(n):
        r, _ = perguntar(pergunta_do_teste(marcador))
        if not (r.status_code == 503 and "configura" in r.json().get("erro", "")):
            apoio.falhou(f"esperava 503 de configuracao, veio {r.status_code}: {r.text[:160]}")
    conferir(chamadas_ao_llm() == n, f"{n} perguntas -> {n} chamadas ao LLM (nenhuma retentativa)")
    conferir(disjuntor_llm() == "fechado", f"circuito segue fechado apos {n} erros permanentes")

    print("3. Erro TRANSITORIO (HTTP 500): retentativas, degradacao elegante e abertura do circuito")
    llm("/modo/erro")
    llm("/reset")
    r, _ = perguntar(pergunta_do_teste(marcador))
    corpo = r.json()
    conferir(r.status_code == 503 and corpo.get("degradado"), "falha vira 503 'degradado' repassado pelo gateway")
    conferir(bool(corpo.get("contexto")) and bool(corpo.get("fontes")),
             "mesmo degradado, entrega os trechos e as fontes recuperados")
    conferir(chamadas_ao_llm() == TENTATIVAS,
             f"{TENTATIVAS} tentativas ao LLM (o gateway NAO multiplica retentativas de resposta degradada)")
    conferir(disjuntor_llm() == "aberto", f"circuito abriu apos {LIMITE_FALHAS} falhas seguidas")

    print("4. Circuito aberto: recusa imediata, o LLM nem e chamado")
    llm("/reset")
    r, duracao = perguntar(pergunta_do_teste(marcador))
    conferir(r.status_code == 503 and duracao < 1.0, f"503 em {duracao:.2f}s")
    conferir(chamadas_ao_llm() == 0, "0 chamadas ao LLM com o circuito aberto")

    print("5. Recuperacao: o LLM volta e o circuito fecha sozinho (meio-aberto -> fechado)")
    fechar_circuito_llm(marcador)

    print(f"6. LLM lento (acima do tempo-limite de {TIMEOUT_LLM:g}s)")
    llm("/modo/lento")
    llm("/reset")
    r, duracao = perguntar(pergunta_do_teste(marcador))
    conferir(r.status_code == 503 and duracao >= TIMEOUT_LLM,
             f"503 apos {duracao:.1f}s: o tempo-limite impede ficar pendurado")
    conferir(chamadas_ao_llm() == TENTATIVAS, f"cada tentativa estourou o tempo-limite ({TENTATIVAS} chamadas)")
    fechar_circuito_llm(marcador)


def cenarios_infra():
    print("7. Servico de inferencia fora do ar: circuit breaker do GATEWAY")
    apoio.docker_compose("stop", "inferencia")
    try:
        r = apoio.chamar("post", "/predict", json={"texto": "teste"})
        conferir(r.status_code == 503, f"/predict responde 503 com a inferencia fora (veio {r.status_code})")
        esperar(lambda: apoio.chamar("post", "/predict", json={"texto": "teste"}).status_code == 503
                and disjuntor_gateway("inferencia") == "aberto",
                "o disjuntor do gateway abrir", 30, intervalo=0.2)
        inicio = time.time()
        r = apoio.chamar("post", "/predict", json={"texto": "teste"})
        duracao = time.time() - inicio
        conferir(r.status_code == 503 and "Retry-After" in r.headers and duracao < 0.5,
                 f"circuito aberto: 503 imediato ({duracao * 1000:.0f} ms) com Retry-After")
        r = apoio.enfileirar("servico excelente")
        conferir(r.status_code == 202, "o caminho assincrono continua aceitando pedidos (vao para a fila)")
        job_id = r.json()["job_id"]
    finally:
        apoio.docker_compose("start", "inferencia")
    esperar(lambda: apoio.chamar("post", "/predict", json={"texto": "bom"}).status_code == 200,
            "o gateway voltar a responder 200 apos a inferencia subir", 120, intervalo=1)
    conferir(disjuntor_gateway("inferencia") == "fechado", "inferencia voltou e o circuito fechou sozinho")
    final = apoio.esperar_job(job_id, 120)
    conferir(final["status"] == "concluido",
             "o job enfileirado durante a queda foi processado depois (nao se perdeu)")

    print("8. Worker parado: a fila segura os pedidos")
    apoio.docker_compose("stop", "worker")
    try:
        job_id = apoio.enfileirar("atendimento ruim").json()["job_id"]
        time.sleep(2)
        conferir(apoio.resultado(job_id).json()["status"] == "enfileirado",
                 "sem worker, o job espera na fila (status enfileirado)")
    finally:
        apoio.docker_compose("start", "worker")
    final = apoio.esperar_job(job_id, 90)
    conferir(final["status"] == "concluido", "worker reiniciado consumiu a fila e concluiu o job")

    print("9. Redis fora do ar: degrada sem derrubar o gateway")
    apoio.docker_compose("stop", "redis")
    try:
        r = apoio.enfileirar("teste")
        conferir(r.status_code == 503, f"/async/predict responde 503 com a fila fora (veio {r.status_code})")
        r = apoio.chamar("post", "/predict", json={"texto": "otimo"})
        conferir(r.status_code == 200, "/predict sincrono segue funcionando (rate limit cai para memoria)")
    finally:
        apoio.docker_compose("start", "redis")
    esperar(lambda: apoio.enfileirar("teste").status_code == 202, "o Redis voltar a aceitar jobs", 90, intervalo=1)
    conferir(True, "Redis voltou e o gateway reconectou sozinho")


def main():
    usar_docker = "--docker" in sys.argv[1:]
    apoio.esperar_gateway()
    try:
        llm("/modo", "get")
    except requests.RequestException:
        apoio.falhou("LLM falso fora do ar: suba com --env-file .env.demo --profile demo")
    doc, marcador = apoio.novo_documento()
    apoio.indexar([doc])
    apoio.esperar_indexado(doc["origem"])

    cenarios_llm(marcador)
    if usar_docker:
        cenarios_infra()
    else:
        print("\n(cenarios de infraestrutura - inferencia, worker e Redis - pulados: use --docker)")
    llm("/modo/ok")
    print("\nOK: resiliencia funcionando")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as erro:
        apoio.falhou(f"nao consegui falar com um dos servicos: {erro}")
