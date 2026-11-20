"""
Rastreamento distribuido com OpenTelemetry (Aula 15), compartilhado por todos os servicos.

Cobre o caminho inteiro de uma requisicao:
  gateway --HTTP--> inferencia / vetorial          (contexto no header W3C `traceparent`)
  gateway / vetorial --fila Redis--> worker        (contexto DENTRO da mensagem, campo `_trace`)

HTTP: a instrumentacao automatica do FastAPI (servidor) e do requests (cliente) cria os spans e
propaga o `traceparent` sozinha. Fila: o Redis nao tem headers, entao quem publica chama
`injetar(mensagem)` e o worker chama `extrair(mensagem)` para continuar o MESMO trace.

Para onde os spans vao (variaveis de ambiente):
  OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318   -> exporta via OTLP/HTTP (Jaeger no compose)
  RASTREAMENTO_CONSOLE=true                         -> imprime os spans no log (depuracao)
  nenhuma das duas                                  -> spans so em memoria; o trace_id continua
                                                       sendo gerado e aparece nos logs e no
                                                       header X-Trace-ID (ex.: no Render)
Nunca coloque dados pessoais (texto do usuario, senha, token) em atributos de span.
"""
import logging
import os

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

log = logging.getLogger("rastreamento")

CAMPO_CONTEXTO = "_trace"   # chave usada para carregar o contexto dentro das mensagens da fila
_configurado = False


def configurar(nome_servico: str, app=None):
    """
    Liga o OpenTelemetry no processo (idempotente). Se `app` (FastAPI) for informado,
    instrumenta as rotas. Sempre instrumenta o cliente `requests`. Devolve um tracer.
    """
    global _configurado
    if not _configurado:
        nome = os.getenv("OTEL_SERVICE_NAME", nome_servico)
        provedor = TracerProvider(resource=Resource.create({"service.name": nome}))
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
        if endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            provedor.add_span_processor(BatchSpanProcessor(
                OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces", timeout=3)))
            log.info("rastreamento: exportando spans via OTLP para %s", endpoint)
        if os.getenv("RASTREAMENTO_CONSOLE", "").lower() in ("1", "true", "sim"):
            provedor.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provedor)

        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        _configurado = True

    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app, excluded_urls="saude")
    return trace.get_tracer(nome_servico)


def tracer(nome: str):
    return trace.get_tracer(nome)


def trace_id_atual() -> str:
    """trace_id do span ativo em hexadecimal (32 caracteres), ou '' se nao houver span."""
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else ""


def injetar(mensagem: dict) -> dict:
    """Grava o contexto do span ativo dentro da mensagem (produtor da fila). Devolve a mensagem."""
    portador: dict = {}
    propagate.inject(portador)
    if portador:
        mensagem[CAMPO_CONTEXTO] = portador
    return mensagem


def extrair(mensagem: dict):
    """Contexto OpenTelemetry carregado na mensagem (consumidor da fila)."""
    portador = mensagem.get(CAMPO_CONTEXTO) if isinstance(mensagem, dict) else None
    return propagate.extract(portador if isinstance(portador, dict) else {})


def anexar(ctx):
    """Torna `ctx` o contexto ativo; devolve o token para `desanexar`."""
    return otel_context.attach(ctx)


def desanexar(token) -> None:
    otel_context.detach(token)
