"""
API Gateway - porta de entrada unica da plataforma.
Implementa:
- TAREFA 1: Autenticacao JWT (/login e protecao de rotas)
- TAREFA 2: Rate limit por cliente por minuto
- TAREFA 6: Resiliencia (Timeout, Retry com Backoff Exponencial e Circuit Breaker)
- TAREFA 7: Observabilidade (Rastreamento distribuido nativo via X-Trace-ID: Gateway -> Fila -> Worker)
"""
import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
import redis
import requests
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel

# --- Configuracoes via Variaveis de Ambiente ---
URL_INFERENCIA = os.getenv("URL_INFERENCIA", "http://localhost:8001")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JWT_SEGREDO = os.getenv("JWT_SEGREDO", "troque-este-segredo-super-seguro-32chars")
JWT_ALGORITMO = "HS256"
JWT_EXPIRACAO_MIN = int(os.getenv("JWT_EXPIRACAO_MIN", "60"))
USUARIO_PADRAO = os.getenv("USUARIO_PADRAO", "admin")
SENHA_PADRAO = os.getenv("SENHA_PADRAO", "admin123")
RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "30"))

# --- Configuracao de Logs (Sem dados pessoais - LGPD) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("gateway")

app = FastAPI(title="Gateway - Plataforma IA C3.A2", version="1.0.0")

# Cliente Redis para fila assincrona e leitura de resultados
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# --- Modelos Pydantic ---
class Credenciais(BaseModel):
    usuario: str
    senha: str


class Entrada(BaseModel):
    texto: str


# --- TAREFA 6: Circuit Breaker ---
class CircuitBreaker:
    def __init__(self, limite_falhas: int = 3, tempo_recuperacao: int = 15):
        self.limite_falhas = limite_falhas
        self.tempo_recuperacao = tempo_recuperacao
        self.falhas = 0
        self.estado = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.aberto_em = 0.0
        self._lock = threading.Lock()

    def permitir_requisicao(self) -> bool:
        with self._lock:
            if self.estado == "CLOSED":
                return True
            if self.estado == "OPEN":
                if time.time() - self.aberto_em >= self.tempo_recuperacao:
                    self.estado = "HALF_OPEN"
                    logger.info("[circuit-breaker] Transicao OPEN -> HALF_OPEN")
                    return True
                return False
            return True  # HALF_OPEN

    def registrar_sucesso(self):
        with self._lock:
            if self.estado != "CLOSED":
                logger.info("[circuit-breaker] Servico recuperado -> CLOSED")
            self.falhas = 0
            self.estado = "CLOSED"

    def registrar_falha(self):
        with self._lock:
            self.falhas += 1
            if self.falhas >= self.limite_falhas or self.estado == "HALF_OPEN":
                self.estado = "OPEN"
                self.aberto_em = time.time()
                logger.warning(
                    f"[circuit-breaker] Circuito ABERTO apos {self.falhas} falhas."
                )


circuit_breaker = CircuitBreaker(limite_falhas=3, tempo_recuperacao=15)


def chamar_inferencia_resiliente(payload: dict, trace_id: str) -> dict:
    """Executa chamada HTTP com Circuit Breaker, Timeout e Retry com Backoff Exponencial."""
    if not circuit_breaker.permitir_requisicao():
        raise HTTPException(
            status_code=503,
            detail="Circuit Breaker OPEN: servico de inferencia temporariamente isolado.",
        )

    max_tentativas = 3
    backoff_base = 0.5  # segundos (0.5s, 1.0s, 2.0s)
    ultimo_erro = None
    headers = {"X-Trace-ID": trace_id}

    for tentativa in range(1, max_tentativas + 1):
        try:
            resposta = requests.post(
                f"{URL_INFERENCIA}/predict",
                json=payload,
                headers=headers,
                timeout=3.0,  # Tempo-limite estrito (Aula 11)
            )
            resposta.raise_for_status()
            circuit_breaker.registrar_sucesso()
            return resposta.json()
        except requests.RequestException as exc:
            ultimo_erro = exc
            logger.warning(
                f"trace_id={trace_id} [resiliencia] Tentativa {tentativa}/{max_tentativas} falhou: {type(exc).__name__}"
            )
            if tentativa < max_tentativas:
                espera = backoff_base * (2 ** (tentativa - 1))
                time.sleep(espera)

    circuit_breaker.registrar_falha()
    raise HTTPException(
        status_code=503,
        detail=f"Servico de inferencia indisponivel apos {max_tentativas} tentativas: {ultimo_erro}",
    )


# --- TAREFA 2: Rate Limit por Cliente (Janela Deslizante de 60s) ---
historico_requisicoes: dict[str, deque] = defaultdict(deque)
rate_lock = threading.Lock()


def verificar_rate_limit(cliente_id: str):
    agora = time.time()
    janela = 60.0
    with rate_lock:
        fila = historico_requisicoes[cliente_id]
        while fila and agora - fila[0] > janela:
            fila.popleft()
        if len(fila) >= RATE_LIMIT_RPM:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit excedido: maximo de {RATE_LIMIT_RPM} requisicoes/minuto.",
            )
        fila.append(agora)


# --- TAREFA 1: Autenticacao JWT ---
def gerar_token(usuario: str) -> str:
    agora = datetime.now(timezone.utc)
    payload = {
        "sub": usuario,
        "iat": agora,
        "exp": agora + timedelta(minutes=JWT_EXPIRACAO_MIN),
    }
    return jwt.encode(payload, JWT_SEGREDO, algorithm=JWT_ALGORITMO)


def validar_token_e_rate_limit(
    authorization: Optional[str] = Header(default=None),
) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token JWT ausente ou formato invalido (use 'Bearer <token>').",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.split(" ", 1)[1].strip()
    try:
        dados = jwt.decode(token, JWT_SEGREDO, algorithms=[JWT_ALGORITMO])
        usuario = dados.get("sub")
        if not usuario:
            raise HTTPException(status_code=401, detail="Token sem identificador.")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token JWT expirado.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token JWT invalido.")

    # Aplica rate limit por cliente autenticado
    verificar_rate_limit(usuario)
    return usuario


# --- TAREFA 7: Middleware de Rastreamento Distribuido (X-Trace-ID) ---
@app.middleware("http")
async def middleware_rastreamento(request: Request, call_next):
    inicio = time.time()
    trace_id = request.headers.get("X-Trace-ID") or str(uuid.uuid4())
    request.state.trace_id = trace_id

    resposta = await call_next(request)
    ms = (time.time() - inicio) * 1000

    resposta.headers["X-Trace-ID"] = trace_id
    logger.info(
        f"trace_id={trace_id} method={request.method} "
        f"path={request.url.path} status={resposta.status_code} latency_ms={ms:.1f}"
    )
    return resposta


# --- Rotas ---
@app.get("/saude")
def saude():
    return {
        "status": "ok",
        "circuit_breaker": circuit_breaker.estado,
    }


@app.post("/login")
def login(credenciais: Credenciais):
    if credenciais.usuario != USUARIO_PADRAO or credenciais.senha != SENHA_PADRAO:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais invalidas.",
        )
    token = gerar_token(credenciais.usuario)
    return {"access_token": token, "token_type": "bearer"}


@app.post("/predict")
def predict(
    entrada: Entrada,
    request: Request,
    usuario: str = Depends(validar_token_e_rate_limit),
):
    """Fluxo sincrono: Gateway -> Inferencia (com Resiliencia + Trace ID)."""
    trace_id = request.state.trace_id
    return chamar_inferencia_resiliente(entrada.model_dump(), trace_id)


@app.post("/async/predict", status_code=202)
def async_predict(
    entrada: Entrada,
    request: Request,
    usuario: str = Depends(validar_token_e_rate_limit),
):
    """Fluxo assincrono rastreado: Gateway -> Fila Redis -> Worker."""
    trace_id = request.state.trace_id
    job_id = str(uuid.uuid4())

    mensagem = {
        "job_id": job_id,
        "texto": entrada.texto,
        "trace_id": trace_id,
        "criado_em": datetime.now(timezone.utc).isoformat(),
    }
    try:
        redis_client.lpush("fila_inferencia", json.dumps(mensagem))
        logger.info(f"trace_id={trace_id} job_id={job_id} acao=enfileirado")
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Fila indisponivel: {exc}")

    return {
        "job_id": job_id,
        "status": "enfileirado",
        "trace_id": trace_id,
        "consultar_em": f"/resultados/{job_id}",
    }


@app.get("/resultados/{job_id}")
def obter_resultado(job_id: str, usuario: str = Depends(validar_token_e_rate_limit)):
    """Consulta o resultado processado pelo Worker."""
    try:
        dados = redis_client.get(f"resultado:{job_id}")
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Erro ao consultar cache: {exc}")

    if not dados:
        return {"job_id": job_id, "status": "pendente_ou_expirado"}
    return json.loads(dados)