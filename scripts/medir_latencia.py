"""
Mede as latencias citadas em docs/RELATORIO_TRADEOFFS.md, sempre pelo gateway.

Uso:  python scripts/medir_latencia.py [repeticoes]
      GATEWAY_URL=https://<sua-url>.onrender.com SENHA=<senha> python scripts/medir_latencia.py 15
Dica: localmente suba com RATE_LIMIT_RPM=1000 para nao esbarrar no limite de taxa
(o script respeita o 429, mas a espera entraria na medida).
"""
import statistics
import sys
import time

import apoio


def percentis(amostras):
    ordenadas = sorted(amostras)
    p95 = ordenadas[min(len(ordenadas) - 1, round(0.95 * (len(ordenadas) - 1)))]
    return statistics.median(ordenadas), p95


def cronometrar(funcao):
    inicio = time.perf_counter()
    resultado = funcao()
    return (time.perf_counter() - inicio) * 1000, resultado


def main(n):
    apoio.esperar_gateway()
    login = [cronometrar(apoio.login)[0] for _ in range(min(n, 8))]   # /login tem limite por IP
    perguntar = [cronometrar(lambda: apoio.perguntar("o que diz o teorema CAP?"))[0] for _ in range(n)]
    predict = [cronometrar(lambda: apoio.chamar("post", "/predict", json={"texto": "bom"}))[0]
               for _ in range(n)]
    aceite, ate_concluir = [], []
    for _ in range(n):
        inicio = time.perf_counter()
        r = apoio.enfileirar("servico excelente")
        aceite.append((time.perf_counter() - inicio) * 1000)
        while apoio.resultado(r.json()["job_id"]).json().get("status") == "enfileirado":
            time.sleep(0.02)
        ate_concluir.append((time.perf_counter() - inicio) * 1000)

    doc, _ = apoio.novo_documento()
    inicio = time.perf_counter()
    apoio.indexar([doc])
    while apoio.contar_origem(doc["origem"]) == 0:
        time.sleep(0.05)
    indexacao = (time.perf_counter() - inicio) * 1000

    print(f"Alvo: {apoio.GATEWAY_URL}  (repeticoes: {n})")
    for nome, amostras in (("POST /login", login), ("POST /predict", predict),
                           ("POST /perguntar", perguntar), ("POST /async/predict (202)", aceite),
                           ("202 -> concluido", ate_concluir)):
        p50, p95 = percentis(amostras)
        print(f"{nome:28s} p50={p50:7.1f} ms   p95={p95:7.1f} ms")
    print(f"{'/indexar -> visivel na busca':28s} {indexacao:7.1f} ms (inclui o polling de /documentos)")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 20)
