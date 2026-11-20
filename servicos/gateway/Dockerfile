# API Gateway (unico servico publico). Contexto de build: a raiz do repositorio.
#   docker build -f servicos/gateway/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# dependencias primeiro: a camada fica em cache enquanto o requirements nao mudar
COPY servicos/gateway/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY servicos/__init__.py servicos/__init__.py
COPY servicos/comum servicos/comum
COPY servicos/gateway servicos/gateway

# nao roda como root: um RCE no processo nao vira root no container
RUN useradd --no-create-home --uid 10001 app
USER app

EXPOSE 8000
# forma shell para expandir ${PORT} (o Render injeta PORT). exec: uvicorn vira o PID 1 e
# recebe o SIGTERM direto (desligamento gracioso). --proxy-headers: IP e scheme reais vindos
# do proxy (rate limit do /login por IP e HSTS). --no-access-log: o middleware ja loga uma
# linha por requisicao sem IP nem query string (LGPD).
CMD exec uvicorn servicos.gateway.app:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips='*' --no-access-log
