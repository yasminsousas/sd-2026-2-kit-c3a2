#!/bin/sh
# Sobe os 3 microsservicos como processos separados dentro do container do Render.
# Se QUALQUER um morrer, o container encerra e o Render o reinicia sozinho.
set -u

PORTA_PUBLICA="${PORT:-10000}"

# Servicos internos: escutam so em 127.0.0.1 (nao ficam expostos na internet).
export URL_INFERENCIA="${URL_INFERENCIA:-http://127.0.0.1:8001}"

echo "[start] subindo inferencia em 127.0.0.1:8001"
(cd /app/inferencia && OTEL_SERVICE_NAME=servico-inferencia \
  exec uvicorn app:app --host 127.0.0.1 --port 8001) &
PID_INFERENCIA=$!

# O worker tem um /saude proprio; PORT precisa ser outra, senao briga com o gateway.
echo "[start] subindo worker (saude em 127.0.0.1:8002)"
(cd /app/worker && PORT=8002 OTEL_SERVICE_NAME=worker-inferencia \
  exec python -u worker.py) &
PID_WORKER=$!

echo "[start] subindo gateway em 0.0.0.0:${PORTA_PUBLICA}"
(cd /app/gateway && OTEL_SERVICE_NAME=api-gateway \
  exec uvicorn app:app --host 0.0.0.0 --port "${PORTA_PUBLICA}" --proxy-headers --forwarded-allow-ips='*') &
PID_GATEWAY=$!

encerrar() {
  echo "[start] encerrando servicos"
  kill -TERM "$PID_GATEWAY" "$PID_WORKER" "$PID_INFERENCIA" 2>/dev/null
  wait
  exit 0
}
trap encerrar TERM INT

# Vigia os 3 processos: se um cair, derruba o container (o Render reinicia).
while kill -0 "$PID_INFERENCIA" 2>/dev/null \
   && kill -0 "$PID_WORKER" 2>/dev/null \
   && kill -0 "$PID_GATEWAY" 2>/dev/null; do
  sleep 2
done
echo "[start] um dos servicos parou; encerrando o container para o Render reiniciar"
kill -TERM "$PID_GATEWAY" "$PID_WORKER" "$PID_INFERENCIA" 2>/dev/null
wait
exit 1
