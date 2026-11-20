# Servico de inferencia. Contexto de build: a raiz do repositorio
#   docker build -f servicos/inferencia/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# dependencias primeiro: a camada fica em cache enquanto o requirements nao mudar
COPY servicos/inferencia/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY servicos/__init__.py servicos/__init__.py
COPY servicos/comum servicos/comum
COPY servicos/inferencia servicos/inferencia

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER app

# --no-access-log: o access log do uvicorn grava o IP do cliente (dado pessoal)
EXPOSE 8001
CMD ["uvicorn", "servicos.inferencia.app:app", "--host", "0.0.0.0", "--port", "8001", "--no-access-log"]
