# Imagem UNICA para o deploy no Render (plano gratis).
#
# Por que uma imagem so: no plano gratis do Render cada servico "dorme" apos
# 15 min sem requisicoes e as 750 h/mes nao cobrem 3 servicos ligados. O worker
# nunca recebe requisicoes, entao dormiria e a fila pararia.
#
# Aqui os 3 microsservicos continuam sendo PROCESSOS SEPARADOS, que conversam
# por HTTP (gateway -> inferencia, worker -> inferencia) e pela fila Redis
# (gateway -> worker). O Redis e o Key Value gratis do Render (externo).
# Localmente, o docker-compose.yml sobe cada servico em seu proprio container.
#
# Build a partir da RAIZ do repositorio:
#   docker build -f deploy/render/Dockerfile -t plataforma-ia .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY gateway/requirements.txt    /tmp/req-gateway.txt
COPY inferencia/requirements.txt /tmp/req-inferencia.txt
COPY worker/requirements.txt     /tmp/req-worker.txt
RUN pip install --no-cache-dir -r /tmp/req-gateway.txt \
        -r /tmp/req-inferencia.txt -r /tmp/req-worker.txt

COPY gateway/    /app/gateway/
COPY inferencia/ /app/inferencia/
COPY worker/     /app/worker/
COPY deploy/render/start.sh /app/start.sh
RUN chmod +x /app/start.sh && useradd --create-home --uid 10001 servico
USER servico

# O Render informa a porta publica na variavel PORT (padrao 10000).
ENV PORT=10000
EXPOSE 10000

CMD ["/app/start.sh"]
