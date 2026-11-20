"""
Worker Assincrono.
Consome tarefas da fila Redis, extrai o X-Trace-ID do Gateway,
chama o servico de inferencia com retry/timeout e persiste o resultado com TTL (LGPD).
Inclui endpoint /saude leve para compatibilidade com Render Web Service Free Tier.
"""
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import redis
import requests

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
URL_INFERENCIA = os.getenv("URL_INFERENCIA", "http://localhost:8001")
TTL_RESULTADOS_SEG = int(os.getenv("TTL_RESULTADOS_SEG", "3600"))
PORT = int(os.getenv("PORT", "8002"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("worker")


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok","servico":"worker"}')

    def log_message(self, format, *args):
        pass  # Evita poluir os logs com healthchecks


def iniciar_healthcheck():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


def processar_mensagem(r: redis.Redis, raw_msg: str):
    dados = json.loads(raw_msg)
    job_id = dados["job_id"]
    texto = dados["texto"]
    trace_id = dados.get("trace_id") or str(uuid.uuid4())

    logger.info(f"trace_id={trace_id} job_id={job_id} acao=consumido_da_fila")

    headers_inferencia = {"X-Trace-ID": trace_id}
    resultado_ia = None

    for tentativa in range(1, 4):
        try:
            resp = requests.post(
                f"{URL_INFERENCIA}/predict",
                json={"texto": texto},
                headers=headers_inferencia,
                timeout=5.0,
            )
            resp.raise_for_status()
            resultado_ia = resp.json()
            break
        except requests.RequestException as exc:
            logger.warning(
                f"trace_id={trace_id} job_id={job_id} tentativa={tentativa} erro={type(exc).__name__}"
            )
            if tentativa < 3:
                time.sleep(0.5 * (2 ** (tentativa - 1)))

    if resultado_ia is None:
        saida = {
            "job_id": job_id,
            "status": "falha",
            "trace_id": trace_id,
            "detalhe": "Falha ao processar inferencia apos retentativas.",
        }
    else:
        saida = {
            "job_id": job_id,
            "status": "concluido",
            "trace_id": trace_id,
            "resultado": resultado_ia,
            "concluido_em": datetime.now(timezone.utc).isoformat(),
        }

    r.setex(f"resultado:{job_id}", TTL_RESULTADOS_SEG, json.dumps(saida))
    logger.info(
        f"trace_id={trace_id} job_id={job_id} status={saida['status']} ttl={TTL_RESULTADOS_SEG}s"
    )


def main():
    threading.Thread(target=iniciar_healthcheck, daemon=True).start()
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Worker iniciado. Aguardando mensagens em 'fila_inferencia'...")
    while True:
        try:
            item = r.brpop("fila_inferencia", timeout=5)
            if item:
                _, raw_msg = item
                processar_mensagem(r, raw_msg)
        except redis.RedisError as exc:
            logger.error(f"Erro de conexao com Redis: {exc}")
            time.sleep(2)
        except Exception as exc:
            logger.error(f"Erro inesperado no worker: {exc}")


if __name__ == "__main__":
    main()