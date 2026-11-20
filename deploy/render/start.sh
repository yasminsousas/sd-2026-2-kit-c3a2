#!/bin/sh
# Sobe os 4 microsservicos como processos separados dentro do container do Render.
# Se QUALQUER um morrer, o container encerra e o Render o reinicia sozinho.
set -u
cd /app

PORTA_PUBLICA="${PORT:-10000}"

# Servicos internos escutam so em 127.0.0.1: nao ficam expostos na internet.
export URL_INFERENCIA="${URL_INFERENCIA:-http://127.0.0.1:8001}"
export URL_VETORIAL="${URL_VETORIAL:-http://127.0.0.1:8002}"

echo "[start] vetorial em 127.0.0.1:8002"
OTEL_SERVICE_NAME=vetorial uvicorn servicos.vetorial.app:app --host 127.0.0.1 --port 8002 --no-access-log &
PID_VETORIAL=$!

echo "[start] inferencia em 127.0.0.1:8001"
OTEL_SERVICE_NAME=inferencia uvicorn servicos.inferencia.app:app --host 127.0.0.1 --port 8001 --no-access-log &
PID_INFERENCIA=$!

echo "[start] worker (filas: ${WORKER_FILAS:-inferencia,ingestao})"
OTEL_SERVICE_NAME=worker python -m servicos.worker.worker &
PID_WORKER=$!

echo "[start] gateway em 0.0.0.0:${PORTA_PUBLICA}"
OTEL_SERVICE_NAME=api-gateway uvicorn servicos.gateway.app:app --host 0.0.0.0 \
  --port "${PORTA_PUBLICA}" --proxy-headers --forwarded-allow-ips="*" --no-access-log &
PID_GATEWAY=$!

TODOS="$PID_GATEWAY $PID_WORKER $PID_INFERENCIA $PID_VETORIAL"

encerrar() {
  echo "[start] encerrando servicos"
  kill -TERM $TODOS 2>/dev/null
  wait
  exit 0
}
trap encerrar TERM INT

# Vigia os processos: se um cair, derruba o container (o Render reinicia).
while :; do
  for pid in $TODOS; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[start] o processo $pid parou; encerrando o container para o Render reiniciar"
      kill -TERM $TODOS 2>/dev/null
      wait
      exit 1
    fi
  done
  sleep 2
done
