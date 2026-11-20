# Imagem UNICA para o deploy no Render (plano gratis).
#
# Por que uma imagem so: no plano gratis cada servico "dorme" apos 15 min sem requisicoes
# e as 750 h/mes da conta nao cobrem 4 servicos ligados o mes todo. O worker nunca recebe
# requisicoes HTTP, entao dormiria e as filas parariam.
#
# Os 4 microsservicos continuam sendo PROCESSOS SEPARADOS (start.sh), com o mesmo codigo
# e as mesmas variaveis do docker-compose, e conversam por HTTP (gateway -> inferencia,
# gateway -> vetorial, inferencia -> vetorial) e pelas filas no Redis (Key Value do Render).
# Localmente, o docker-compose.yml sobe cada servico em seu proprio container.
#
# Build a partir da RAIZ do repositorio:
#   docker build -f deploy/render/Dockerfile -t plataforma-ia .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY servicos/gateway/requirements.txt    /tmp/req-gateway.txt
COPY servicos/inferencia/requirements.txt /tmp/req-inferencia.txt
COPY servicos/vetorial/requirements.txt   /tmp/req-vetorial.txt
COPY servicos/worker/requirements.txt     /tmp/req-worker.txt
RUN pip install --no-cache-dir -r /tmp/req-gateway.txt -r /tmp/req-inferencia.txt \
        -r /tmp/req-vetorial.txt -r /tmp/req-worker.txt

COPY servicos/   /app/servicos/
COPY documentos/ /app/documentos/
COPY deploy/render/start.sh /app/start.sh
RUN chmod +x /app/start.sh \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /app/dados && chown app /app/dados
USER app

# O Render informa a porta publica na variavel PORT (padrao 10000).
ENV PORT=10000 \
    DATA_DIR=/app/dados \
    SEMEAR_DOCUMENTOS=/app/documentos
EXPOSE 10000

CMD ["/app/start.sh"]
