# Worker (consumidores das filas de ingestao e inferencia). Sem HTTP.
# Contexto de build: a raiz do repositorio
#   docker build -f servicos/worker/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DATA_DIR=/dados
WORKDIR /app

# dependencias primeiro: a camada fica em cache enquanto o requirements nao mudar
COPY servicos/worker/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY servicos/__init__.py servicos/__init__.py
COPY servicos/comum servicos/comum
COPY servicos/worker servicos/worker
# documentos de exemplo: usados quando SEMEAR_DOCUMENTOS=/app/documentos
COPY documentos documentos

# /dados ja pertence ao usuario: um volume nomeado novo herda esse dono
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /dados && chown app:app /dados
USER app

CMD ["python", "-m", "servicos.worker.worker"]
