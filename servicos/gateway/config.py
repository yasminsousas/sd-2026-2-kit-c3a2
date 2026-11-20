"""
Configuracao do gateway, toda lida de variaveis de ambiente (12-factor).

Fica num modulo separado para que a validacao dos segredos rode UMA vez, na importacao:
em producao uma configuracao insegura derruba o processo ao iniciar (falha cedo e alto),
em vez de subir aceitando tokens assinados com um segredo fraco ou conhecido.
"""
import logging
import os
import secrets

log = logging.getLogger("gateway.config")

TAMANHO_MINIMO_SEGREDO = 32   # HS256: segredo curto permite forca bruta offline do token


class ConfiguracaoInsegura(RuntimeError):
    """Configuracao que nao pode ir para producao (segredo fraco, senha padrao...)."""


def _int(nome, padrao):
    return int(os.getenv(nome, str(padrao)))


def _float(nome, padrao):
    return float(os.getenv(nome, str(padrao)))


def em_producao(ambiente: str) -> bool:
    return ambiente.strip().lower() in ("producao", "production", "prod")


def carregar_segredo_jwt(valor, ambiente: str) -> str:
    """
    Segredo de assinatura dos JWT.
    - producao: ausente ou com menos de 32 caracteres -> ConfiguracaoInsegura (nao sobe);
    - desenvolvimento: segredo curto e aceito com aviso; ausente gera um aleatorio por
      processo (tokens deixam de valer ao reiniciar, mas nunca existe um segredo "padrao"
      conhecido por todo mundo que leu o codigo).
    """
    valor = (valor or "").strip()
    if len(valor) >= TAMANHO_MINIMO_SEGREDO:
        return valor
    if em_producao(ambiente):
        raise ConfiguracaoInsegura(
            f"JWT_SEGREDO ausente ou com menos de {TAMANHO_MINIMO_SEGREDO} caracteres "
            "em producao. Gere um com: python -c 'import secrets; print(secrets.token_urlsafe(48))'")
    if not valor:
        log.warning("JWT_SEGREDO nao definido: usando segredo aleatorio deste processo "
                    "(somente desenvolvimento; tokens expiram ao reiniciar)")
        return secrets.token_urlsafe(48)
    log.warning("JWT_SEGREDO com menos de %d caracteres: aceito so porque AMBIENTE "
                "nao e producao", TAMANHO_MINIMO_SEGREDO)
    return valor


def carregar_senha(valor, ambiente: str) -> str:
    """Em producao a senha precisa vir do ambiente: a senha de exemplo do codigo e publica."""
    if valor:
        return valor
    if em_producao(ambiente):
        raise ConfiguracaoInsegura("SENHA_PADRAO nao definida em producao")
    log.warning("SENHA_PADRAO nao definida: usando a senha de desenvolvimento")
    return "admin123"


AMBIENTE = os.getenv("AMBIENTE", "desenvolvimento")
VERSAO = os.getenv("VERSAO", "3.0.0")

# autenticacao
JWT_SEGREDO = carregar_segredo_jwt(os.getenv("JWT_SEGREDO"), AMBIENTE)
JWT_ALGORITMO = "HS256"
JWT_EMISSOR = "plataforma-ia"
JWT_EXPIRACAO_MIN = _int("JWT_EXPIRACAO_MIN", 60)
USUARIO_PADRAO = os.getenv("USUARIO_PADRAO", "admin")
SENHA_PADRAO = carregar_senha(os.getenv("SENHA_PADRAO"), AMBIENTE)

# limites de taxa (janela fixa de 1 minuto)
RATE_LIMIT_RPM = _int("RATE_LIMIT_RPM", 30)
LOGIN_RATE_LIMIT_RPM = _int("LOGIN_RATE_LIMIT_RPM", 10)
REDIS_ESPERA_RETENTAR = _float("REDIS_ESPERA_RETENTAR", 5)   # s sem tentar o Redis apos falha

# dependencias
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
URL_INFERENCIA = os.getenv("URL_INFERENCIA", "http://localhost:8001").rstrip("/")
URL_VETORIAL = os.getenv("URL_VETORIAL", "http://localhost:8002").rstrip("/")
TTL_RESULTADOS_SEG = _int("TTL_RESULTADOS_SEG", 3600)

# resiliencia das chamadas internas
GATEWAY_TIMEOUT = _float("GATEWAY_TIMEOUT", 5)
# maior que o pior caso da inferencia (3 x LLM_TIMEOUT 10s + backoff 0,5+1s = 31,5s): o gateway
# nao deve desistir (e retentar) enquanto a inferencia ainda trabalha
GATEWAY_TIMEOUT_PERGUNTAR = _float("GATEWAY_TIMEOUT_PERGUNTAR", 40)
GATEWAY_TIMEOUT_SAUDE = _float("GATEWAY_TIMEOUT_SAUDE", 2)
GATEWAY_TENTATIVAS = _int("GATEWAY_TENTATIVAS", 3)
GATEWAY_BACKOFF = _float("GATEWAY_BACKOFF", 0.3)
GATEWAY_LIMITE_FALHAS = _int("GATEWAY_LIMITE_FALHAS", 5)
GATEWAY_ESPERA_CIRCUITO = _float("GATEWAY_ESPERA_CIRCUITO", 30)

# protecao contra corpos gigantes (o maior corpo legitimo e /indexar: 50 x 20k caracteres)
GATEWAY_MAX_CORPO_BYTES = _int("GATEWAY_MAX_CORPO_BYTES", 2 * 1024 * 1024)
