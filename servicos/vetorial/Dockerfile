# Servico vetorial (indexar/buscar). Contexto de build: a raiz do repositorio
#   docker build -f servicos/vetorial/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DATA_DIR=/dados
WORKDIR /app

# dependencias primeiro: a camada fica em cache enquanto o requirements nao mudar
COPY servicos/vetorial/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY servicos/__init__.py servicos/__init__.py
COPY servicos/comum servicos/comum
COPY servicos/vetorial servicos/vetorial

# /dados ja pertence ao usuario: um volume nomeado novo herda esse dono
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /dados && chown app:app /dados
USER app

# --no-access-log: o access log do uvicorn grava o IP do cliente (dado pessoal)
EXPOSE 8002
CMD ["uvicorn", "servicos.vetorial.app:app", "--host", "0.0.0.0", "--port", "8002", "--no-access-log"]
