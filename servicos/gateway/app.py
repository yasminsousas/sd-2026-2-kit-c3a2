"""
API GATEWAY - unica porta publica da plataforma (porta 8000).

Responsabilidades (o resto da plataforma so e alcancavel pela rede interna):
  - AUTENTICACAO: JWT HS256 emitido em /login; toda rota de negocio exige o token;
  - LIMITE DE TAXA: por usuario (rotas protegidas) e por IP (/login, contra forca bruta),
    contado no Redis para valer entre replicas, com fallback em memoria se o Redis cair;
  - RESILIENCIA: chamadas a inferencia e ao vetorial com tempo-limite, retentativa com
    backoff+jitter e um disjuntor por servico (ver resiliencia.py);
  - ASSINCRONO: /async/predict grava o "recibo" do job e publica na fila do worker no mesmo
    pipeline do Redis; /resultados/{job_id} so mostra o job ao proprio dono;
  - RASTREAMENTO: OpenTelemetry; o trace_id volta no header X-Trace-ID e segue para a
    fila dentro da mensagem (`_trace`), ligando gateway -> fila -> worker -> inferencia;
  - PRIVACIDADE (LGPD): log de uma linha por requisicao com metodo, rota, status, latencia
    e trace_id. Nunca corpo, token, senha ou usuario em claro (o usuario vira hash).

Rodar (da raiz do repo):
  uvicorn servicos.gateway.app:app --port 8000 --proxy-headers --forwarded-allow-ips='*'
"""
import json
import logging
import os
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Annotated

import redis
from fastapi import Body, Depends, FastAPI, HTTPException, Path, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from opentelemetry.trace import SpanKind
from pydantic import BaseModel, Field, StringConstraints

from servicos.comum import rastreamento
from servicos.gateway import config as cfg
from servicos.gateway.autenticacao import (TokenInvalido, credenciais_validas, emitir_token,
                                           hash_usuario, validar_token)
from servicos.gateway.limite import Limitador
from servicos.gateway.resiliencia import ClienteServico, ServicoIndisponivel

logging.basicConfig(level=os.getenv("LOG_NIVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")

FILA_INFERENCIA = "fila:inferencia"
FILA_INGESTAO = "fila:ingestao"
PADRAO_BATIMENTO = "worker:batimento:*"
# uuid4 em hex (32) ou no formato canonico com hifens (36)
PADRAO_JOB_ID = r"^([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"

app = FastAPI(
    title="API Gateway - Plataforma de IA (C3.A2)",
    version=cfg.VERSAO,
    description="Porta de entrada unica: autenticacao JWT, limite de taxa, resiliencia e rastreamento.",
)

r = redis.Redis.from_url(cfg.REDIS_URL, decode_responses=True,
                         socket_timeout=2, socket_connect_timeout=2, health_check_interval=30)
limitador = Limitador(lambda: r, espera_retentar=cfg.REDIS_ESPERA_RETENTAR)


def _cliente(nome, url):
    return ClienteServico(nome, url, timeout=cfg.GATEWAY_TIMEOUT,
                          tentativas=cfg.GATEWAY_TENTATIVAS, backoff=cfg.GATEWAY_BACKOFF,
                          limite_falhas=cfg.GATEWAY_LIMITE_FALHAS,
                          espera_circuito=cfg.GATEWAY_ESPERA_CIRCUITO)


cliente_inferencia = _cliente("inferencia", cfg.URL_INFERENCIA)
cliente_vetorial = _cliente("vetorial", cfg.URL_VETORIAL)


# ---------------------------------------------------------------------------------------
# Middleware: trace_id, cabecalhos de seguranca, limite de corpo e log por requisicao.
# Registrado ANTES de rastreamento.configurar(): o middleware do OpenTelemetry entra por
# ultimo e fica mais externo, entao aqui dentro o span da requisicao ja esta ativo.
# ---------------------------------------------------------------------------------------
CABECALHOS_SEGURANCA = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",   # respostas com token/resultados nao vao para cache
}


def _https(request: Request) -> bool:
    """Com --proxy-headers o uvicorn ja ajusta o scheme; o header cobre proxies nao confiados."""
    proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    return request.url.scheme == "https" or proto == "https"


async def middleware_requisicao(request: Request, call_next):
    inicio = time.perf_counter()
    # sem span (ex.: /saude, excluida da instrumentacao) gera um id so para correlacionar
    trace_id = rastreamento.trace_id_atual() or uuid.uuid4().hex
    request.state.trace_id = trace_id

    tamanho = request.headers.get("content-length", "")
    if tamanho.isdigit() and int(tamanho) > cfg.GATEWAY_MAX_CORPO_BYTES:
        resposta = JSONResponse(status_code=413, content={"detail": "corpo da requisicao grande demais"})
    else:
        try:
            resposta = await call_next(request)
        except Exception as erro:
            # so o tipo e as linhas de codigo: a mensagem da excecao pode conter dados do pedido
            log.error("erro nao tratado trace_id=%s tipo=%s\n%s", trace_id, type(erro).__name__,
                      "".join(traceback.format_tb(erro.__traceback__)))
            resposta = JSONResponse(status_code=500,
                                    content={"detail": "erro interno", "trace_id": trace_id})

    resposta.headers["X-Trace-ID"] = trace_id
    for nome, valor in CABECALHOS_SEGURANCA.items():
        resposta.headers.setdefault(nome, valor)
    if _https(request):
        resposta.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    decisao = getattr(request.state, "limite", None)
    if decisao is not None:
        resposta.headers["X-RateLimit-Limit"] = str(decisao.limite)
        resposta.headers["X-RateLimit-Remaining"] = str(decisao.restante)

    rota = request.scope.get("route")
    log.info("requisicao metodo=%s rota=%s status=%d latencia_ms=%.1f trace_id=%s",
             request.method, getattr(rota, "path", "<sem-rota>"), resposta.status_code,
             (time.perf_counter() - inicio) * 1000, trace_id)
    return resposta


app.middleware("http")(middleware_requisicao)
tracer = rastreamento.configurar("api-gateway", app)


# ---------------------------------------------------------------------------------------
# Autenticacao + limite de taxa (dependencias)
# ---------------------------------------------------------------------------------------
portador = HTTPBearer(auto_error=False, description="JWT obtido em POST /login")


def _nao_autorizado(motivo: str, erro_oauth: str | None = None):
    desafio = "Bearer" if erro_oauth is None else f'Bearer error="{erro_oauth}"'
    return HTTPException(status_code=401, detail=motivo, headers={"WWW-Authenticate": desafio})


def _aplicar_limite(request: Request, identificador: str, limite: int):
    decisao = limitador.verificar(identificador, limite)
    request.state.limite = decisao
    if not decisao.permitido:
        raise HTTPException(
            status_code=429,
            detail=f"limite de {limite} requisicoes por minuto excedido; tente de novo em "
                   f"{decisao.retry_after}s",
            headers={"Retry-After": str(decisao.retry_after),
                     "X-RateLimit-Limit": str(limite), "X-RateLimit-Remaining": "0"})


def usuario_atual(request: Request,
                  credenciais: HTTPAuthorizationCredentials | None = Depends(portador)) -> str:
    """Valida o JWT, aplica o limite por usuario e devolve o hash do usuario (o "dono")."""
    if credenciais is None:
        raise _nao_autorizado("token ausente: envie 'Authorization: Bearer <token>'")
    try:
        usuario = validar_token(credenciais.credentials)
    except TokenInvalido as erro:
        raise _nao_autorizado(str(erro), "invalid_token")
    dono = hash_usuario(usuario)
    _aplicar_limite(request, dono, cfg.RATE_LIMIT_RPM)
    return dono


Dono = Annotated[str, Depends(usuario_atual)]


# ---------------------------------------------------------------------------------------
# Modelos (limites de tamanho protegem os servicos internos e o Redis)
# ---------------------------------------------------------------------------------------
def _texto(maximo: int):
    return Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=maximo)]


class Credenciais(BaseModel):
    usuario: _texto(128)
    senha: Annotated[str, StringConstraints(min_length=1, max_length=256)]


class Entrada(BaseModel):
    texto: _texto(5000)


class Pergunta(BaseModel):
    pergunta: _texto(2000)
    top_k: int = Field(default=3, ge=1, le=10)


class Documento(BaseModel):
    texto: _texto(20000)
    origem: _texto(200)


JobId = Annotated[str, Path(pattern=PADRAO_JOB_ID, description="id devolvido por /async/predict")]


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------
def _repassar(cliente: ClienteServico, metodo: str, caminho: str, corpo=None,
              timeout=None, idempotente=True) -> JSONResponse:
    """Chama o servico interno e devolve status + corpo dele (inclusive 4xx e 503 degradado)."""
    try:
        resposta = cliente.chamar(metodo, caminho, corpo, timeout=timeout, idempotente=idempotente)
    except ServicoIndisponivel as erro:
        cabecalhos = {"Retry-After": f"{cliente.disjuntor.espera_segundos:g}"} \
            if erro.circuito_aberto else None
        raise HTTPException(status_code=503, detail=str(erro), headers=cabecalhos)
    return JSONResponse(status_code=resposta.status, content=resposta.corpo)


def _ler_resultado(job_id: str, dono: str) -> dict:
    """Resultado do job, ou 404 se nao existe, expirou OU pertence a outro usuario."""
    try:
        bruto = r.get(f"resultado:{job_id}")
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="armazenamento de resultados indisponivel")
    try:
        dados = json.loads(bruto) if bruto else None
    except ValueError:
        dados = None
    # mesma resposta para "nao existe" e "e de outro": nao confirma a existencia do job
    if not isinstance(dados, dict) or dados.get("dono") != dono:
        raise HTTPException(status_code=404, detail="resultado nao encontrado ou expirado")
    return dados


def _agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------------------
# Rotas publicas
# ---------------------------------------------------------------------------------------
@app.get("/", tags=["publico"])
def raiz():
    rotas = sorted({f"{metodo} {rota.path}" for rota in app.routes
                    for metodo in (getattr(rota, "methods", None) or ())
                    if metodo != "HEAD" and not rota.path.startswith(("/openapi", "/docs", "/redoc"))})
    return {"nome": "api-gateway", "versao": cfg.VERSAO, "documentacao": "/docs", "rotas": rotas}


@app.get("/saude", tags=["publico"])
def saude():
    """Liveness: responde sem tocar em nada (healthcheck do Render/compose)."""
    return {"status": "ok", "servico": "api-gateway", "versao": cfg.VERSAO}


@app.post("/login", tags=["publico"])
def login(credenciais: Credenciais, request: Request):
    ip = request.client.host if request.client else "desconhecido"
    _aplicar_limite(request, "login:" + hash_usuario(ip), cfg.LOGIN_RATE_LIMIT_RPM)
    if not credenciais_validas(credenciais.usuario, credenciais.senha):
        log.warning("login recusado trace_id=%s", request.state.trace_id)
        # generico: nao diz se o erro foi no usuario ou na senha
        raise _nao_autorizado("credenciais invalidas")
    return {"access_token": emitir_token(credenciais.usuario), "token_type": "bearer",
            "expira_em_min": cfg.JWT_EXPIRACAO_MIN}


# ---------------------------------------------------------------------------------------
# Rotas protegidas
# ---------------------------------------------------------------------------------------
@app.get("/saude/detalhada", tags=["operacao"])
def saude_detalhada(dono: Dono):
    """Estado das dependencias (Redis, servicos, disjuntores, workers e filas)."""
    estado_redis = {"status": "ok"}
    try:
        r.ping()
        estado_redis["workers_vivos"] = sum(1 for _ in r.scan_iter(match=PADRAO_BATIMENTO, count=100))
        estado_redis["filas"] = {fila: r.llen(fila) for fila in (FILA_INFERENCIA, FILA_INGESTAO)}
    except redis.RedisError as erro:
        estado_redis = {"status": "indisponivel", "erro": type(erro).__name__}

    with ThreadPoolExecutor(max_workers=2) as executor:   # as duas sondas em paralelo
        futuros = {c.nome: executor.submit(c.saude, cfg.GATEWAY_TIMEOUT_SAUDE)
                   for c in (cliente_inferencia, cliente_vetorial)}
        servicos = {nome: f.result() for nome, f in futuros.items()}

    tudo_ok = estado_redis["status"] == "ok" and all(s["status"] == "ok" for s in servicos.values())
    return {
        "status": "ok" if tudo_ok else "degradado",
        "redis": estado_redis,
        "servicos": servicos,
        "disjuntores": {"inferencia": cliente_inferencia.disjuntor.estado,
                        "vetorial": cliente_vetorial.disjuntor.estado,
                        "redis_rate_limit": limitador.disjuntor.estado},
    }


@app.post("/predict", tags=["inferencia"])
def predict(entrada: Entrada, dono: Dono):
    """Sincrono: repassa a inferencia (resiliente)."""
    return _repassar(cliente_inferencia, "POST", "/predict", {"texto": entrada.texto})


@app.post("/async/predict", status_code=202, tags=["inferencia"])
def async_predict(entrada: Entrada, dono: Dono, request: Request, response: Response):
    """
    Assincrono: grava o recibo `resultado:<job_id>` (status enfileirado) e publica na fila
    no MESMO pipeline transacional - nunca existe job na fila sem recibo, nem recibo sem job.
    """
    job_id = uuid.uuid4().hex
    trace_id = request.state.trace_id
    criado_em = _agora_iso()
    recibo = {"job_id": job_id, "status": "enfileirado", "dono": dono,
              "criado_em": criado_em, "trace_id": trace_id}
    with tracer.start_as_current_span(f"fila.publicar {FILA_INFERENCIA}", kind=SpanKind.PRODUCER) as span:
        span.set_attribute("messaging.system", "redis")
        span.set_attribute("messaging.destination.name", FILA_INFERENCIA)
        span.set_attribute("messaging.message.id", job_id)
        mensagem = rastreamento.injetar({"job_id": job_id, "texto": entrada.texto,
                                         "dono": dono, "criado_em": criado_em})
        try:
            pipe = r.pipeline(transaction=True)
            pipe.set(f"resultado:{job_id}", json.dumps(recibo), ex=cfg.TTL_RESULTADOS_SEG)
            pipe.rpush(FILA_INFERENCIA, json.dumps(mensagem))
            pipe.execute()
        except redis.RedisError as erro:
            log.error("falha ao enfileirar trace_id=%s job_id=%s erro=%s",
                      trace_id, job_id, type(erro).__name__)
            raise HTTPException(status_code=503, detail="fila indisponivel; tente novamente")
    log.info("job enfileirado trace_id=%s job_id=%s tamanho_texto=%d",
             trace_id, job_id, len(entrada.texto))
    consultar_em = f"/resultados/{job_id}"
    response.headers["Location"] = consultar_em
    return {"job_id": job_id, "status": "enfileirado", "trace_id": trace_id,
            "consultar_em": consultar_em}


@app.get("/resultados/{job_id}", tags=["inferencia"])
def obter_resultado(job_id: JobId, dono: Dono):
    dados = _ler_resultado(job_id, dono)
    dados.pop("dono", None)
    return dados


@app.delete("/resultados/{job_id}", status_code=204, tags=["inferencia"])
def apagar_resultado(job_id: JobId, dono: Dono):
    """Direito de eliminacao (LGPD): apaga o resultado do proprio usuario."""
    _ler_resultado(job_id, dono)
    try:
        r.delete(f"resultado:{job_id}")
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="armazenamento de resultados indisponivel")
    log.info("resultado apagado job_id=%s", job_id)
    return Response(status_code=204)


@app.post("/indexar", status_code=202, tags=["documentos"])
def indexar(documentos: Annotated[list[Documento], Body(min_length=1, max_length=50)], dono: Dono):
    """Repassa ao vetorial. Nao idempotente: sem retentativa apos tempo-limite de leitura."""
    corpo = [d.model_dump() for d in documentos]
    return _repassar(cliente_vetorial, "POST", "/indexar", corpo, idempotente=False)


@app.post("/perguntar", tags=["documentos"])
def perguntar(p: Pergunta, dono: Dono):
    """RAG: repassa a inferencia, inclusive o 503 degradado (com contexto) quando o LLM cai."""
    return _repassar(cliente_inferencia, "POST", "/perguntar", p.model_dump(),
                     timeout=cfg.GATEWAY_TIMEOUT_PERGUNTAR)


@app.get("/documentos", tags=["documentos"])
def documentos(dono: Dono):
    return _repassar(cliente_vetorial, "GET", "/documentos")
