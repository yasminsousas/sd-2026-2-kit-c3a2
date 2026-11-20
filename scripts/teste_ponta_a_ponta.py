"""
Teste de ponta a ponta, repetivel, SEMPRE pelo gateway (como um cliente real).

Confere, em ordem:
  1. gateway no ar e rotas protegidas (401 sem token, login JWT);
  2. inferencia sincrona (gateway -> inferencia) com X-Trace-ID;
  3. inferencia assincrona (gateway -> fila -> worker -> inferencia) e o resultado com TTL;
     direito de eliminacao (DELETE /resultados/<id>);
  4. armazenamento vetorial: indexacao pela fila, idempotencia do reenvio;
  5. RAG: pergunta -> resposta fundamentada no documento certo;
  6. rastreamento distribuido: o MESMO trace aparece no Jaeger para gateway, worker e inferencia
     (pulado se o Jaeger nao estiver acessivel, ex.: na URL publica).

Uso:  python scripts/teste_ponta_a_ponta.py
      GATEWAY_URL=https://<sua-url>.onrender.com SENHA=<senha> python scripts/teste_ponta_a_ponta.py
"""
import time

import apoio
from apoio import conferir


def main():
    print(f"Alvo: {apoio.GATEWAY_URL}")
    print("1. gateway no ar e autenticacao JWT")
    apoio.esperar_gateway()
    conferir(True, "GET /saude responde")
    r = apoio.chamar("post", "/predict", autenticado=False, json={"texto": "teste"})
    conferir(r.status_code == 401, f"sem token -> 401 (veio {r.status_code})")
    conferir(apoio.login(apoio.USUARIO, apoio.SENHA + "-errada").status_code == 401, "senha errada -> 401")
    conferir(apoio.login().status_code == 200, "login com as credenciais certas emite o token")

    print("2. inferencia sincrona (gateway -> inferencia)")
    r = apoio.chamar("post", "/predict", json={"texto": "servico excelente, recomendo"})
    conferir(r.status_code == 200, f"/predict responde 200 (veio {r.status_code}: {r.text[:120]})")
    conferir(r.json()["sentimento"] == "positivo", f"sentimento positivo: {r.json()}")
    conferir(len(r.headers.get("X-Trace-ID", "")) == 32, "resposta traz o X-Trace-ID (trace OpenTelemetry)")

    print("3. inferencia assincrona (gateway -> fila -> worker -> inferencia)")
    r = apoio.enfileirar("atendimento pessimo e muito lento")
    conferir(r.status_code == 202, f"/async/predict responde 202 (veio {r.status_code}: {r.text[:120]})")
    job = r.json()
    trace_async = job.get("trace_id") or r.headers.get("X-Trace-ID", "")
    final = apoio.esperar_job(job["job_id"])
    conferir(final["status"] == "concluido", f"o worker concluiu o job: {final.get('status')}")
    conferir(final["resultado"]["sentimento"] == "negativo", f"resultado negativo: {final['resultado']}")
    conferir("dono" not in final, "a resposta nao expoe o identificador do dono")
    conferir(apoio.chamar("delete", f"/resultados/{job['job_id']}").status_code == 204,
             "DELETE /resultados/<id> apaga o resultado (direito de eliminacao)")
    conferir(apoio.resultado(job["job_id"]).status_code == 404, "depois de apagado -> 404")

    print("4. armazenamento vetorial (gateway -> vetorial -> fila -> worker -> indice)")
    doc, marcador = apoio.novo_documento()
    conferir(apoio.contar_origem(doc["origem"]) == 0, "o documento desta execucao ainda nao existe")
    r = apoio.indexar([doc])
    conferir(r.status_code == 202, f"POST /indexar responde 202: foi para a fila (veio {r.status_code})")
    apoio.esperar_indexado(doc["origem"])
    conferir(True, "o worker consumiu a fila e o documento apareceu no indice")
    sentinela, _ = apoio.novo_documento(" (sentinela)")
    apoio.indexar([doc, doc])
    apoio.indexar([sentinela])     # a fila e FIFO: quando a sentinela entra, o reenvio ja passou
    apoio.esperar_indexado(sentinela["origem"])
    conferir(apoio.contar_origem(doc["origem"]) == 1, "reenvio nao duplica (idempotencia)")

    print("5. RAG: pergunta -> resposta fundamentada no documento")
    r, _ = apoio.perguntar(f"o que diz o protocolo {marcador}?")
    conferir(r.status_code == 200, f"/perguntar responde 200 (veio {r.status_code}: {r.text[:160]})")
    corpo = r.json()
    conferir(bool(corpo["resposta"]), "a resposta nao e vazia")
    conferir(corpo["fontes"][:1] == [doc["origem"]], f"a fonte principal e o documento certo: {corpo['fontes']}")
    conferir(marcador in corpo["contexto"][0], "o contexto enviado ao LLM contem o trecho indexado")

    print("6. rastreamento distribuido no Jaeger")
    if not apoio.jaeger_disponivel():
        print(f"   (pulado: Jaeger nao acessivel em {apoio.JAEGER_URL})")
    else:
        esperados = {"api-gateway", "worker", "inferencia"}
        servicos = apoio.esperar(
            lambda: (s := apoio.servicos_no_trace(trace_async)) >= esperados and s,
            f"o trace {trace_async} reunir {sorted(esperados)}", limite=30, intervalo=1)
        conferir(True, f"trace {trace_async[:12]}... cobre {sorted(servicos)} (gateway -> fila -> worker)")

    print("\nOK: plataforma funcionando de ponta a ponta")


if __name__ == "__main__":
    inicio = time.time()
    try:
        main()
    except apoio.requests.RequestException as erro:
        apoio.falhou(f"nao consegui falar com o gateway: {erro}")
    print(f"(duracao: {time.time() - inicio:.1f}s)")
