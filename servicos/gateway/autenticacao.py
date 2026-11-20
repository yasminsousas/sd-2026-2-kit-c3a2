"""
Autenticacao: emissao e validacao de JWT (HS256) e conferencia de credenciais.

Decisoes:
  - o token carrega so o necessario (sub, iat, exp, iss); nada de dado pessoal extra;
  - a validacao EXIGE exp, iat, iss e sub e aceita apenas HS256: sem a lista fixa de
    algoritmos, um token com alg=none ou com outro algoritmo poderia ser aceito;
  - credenciais comparadas com hmac.compare_digest e SEMPRE as duas: o tempo de resposta
    nao revela se o usuario existe nem quantos caracteres da senha estavam certos;
  - o usuario nunca aparece em claro fora daqui: chaves do Redis, campo "dono" dos
    resultados e logs usam hash_usuario() (pseudonimizacao, LGPD).
"""
import hashlib
import hmac
from datetime import datetime, timedelta, timezone

import jwt

from servicos.gateway import config as cfg

FOLGA_RELOGIO_SEG = 10   # tolera pequena diferenca de relogio entre maquinas


class TokenInvalido(Exception):
    """Token ausente, expirado, adulterado ou com claims faltando. A mensagem e segura para o cliente."""


def hash_usuario(usuario: str) -> str:
    """Identificador pseudonimo e estavel do usuario (sha256 truncado em 16 hex)."""
    return hashlib.sha256(usuario.encode("utf-8")).hexdigest()[:16]


def credenciais_validas(usuario: str, senha: str) -> bool:
    usuario_ok = hmac.compare_digest(usuario.encode("utf-8"), cfg.USUARIO_PADRAO.encode("utf-8"))
    senha_ok = hmac.compare_digest(senha.encode("utf-8"), cfg.SENHA_PADRAO.encode("utf-8"))
    return usuario_ok and senha_ok


def emitir_token(usuario: str, agora: datetime | None = None) -> str:
    agora = agora or datetime.now(timezone.utc)
    payload = {
        "sub": usuario,
        "iss": cfg.JWT_EMISSOR,
        "iat": agora,
        "exp": agora + timedelta(minutes=cfg.JWT_EXPIRACAO_MIN),
    }
    return jwt.encode(payload, cfg.JWT_SEGREDO, algorithm=cfg.JWT_ALGORITMO)


def validar_token(token: str) -> str:
    """Devolve o `sub` do token ou levanta TokenInvalido."""
    try:
        dados = jwt.decode(
            token, cfg.JWT_SEGREDO,
            algorithms=[cfg.JWT_ALGORITMO],
            issuer=cfg.JWT_EMISSOR,
            leeway=FOLGA_RELOGIO_SEG,
            options={"require": ["exp", "iat", "iss", "sub"]},
        )
    except jwt.ExpiredSignatureError:
        raise TokenInvalido("token expirado")
    except jwt.InvalidTokenError:
        raise TokenInvalido("token invalido")
    sub = dados.get("sub")
    if not isinstance(sub, str) or not sub:
        raise TokenInvalido("token invalido")
    return sub
