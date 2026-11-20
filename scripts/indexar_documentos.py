"""
Envia os arquivos .txt de uma pasta (padrao: documentos/) para o armazenamento vetorial,
passando pelo gateway (com login JWT).

Cada paragrafo (separado por linha em branco) vira um trecho; a origem e o nome do arquivo,
e e ela que aparece depois em "fontes" na resposta de /perguntar.
Reenviar e seguro: o indice ignora documentos repetidos.

Uso:  python scripts/indexar_documentos.py [pasta]
      GATEWAY_URL=https://<sua-url> SENHA=<senha> python scripts/indexar_documentos.py
"""
import sys
from pathlib import Path

import apoio

PASTA_PADRAO = apoio.RAIZ / "documentos"
LOTE = 50   # limite de documentos por requisicao no gateway


def montar_documentos(pasta: Path) -> list:
    docs = []
    for arquivo in sorted(pasta.glob("*.txt")):
        for paragrafo in arquivo.read_text(encoding="utf-8").split("\n\n"):
            if paragrafo.strip():
                docs.append({"texto": paragrafo.strip(), "origem": arquivo.stem})
    return docs


def main(pasta: Path = PASTA_PADRAO) -> int:
    docs = montar_documentos(pasta)
    if not docs:
        print(f"nenhum .txt encontrado em {pasta}")
        return 0
    if apoio.login().status_code != 200:
        apoio.falhou("login recusado: confira USUARIO e SENHA")
    for i in range(0, len(docs), LOTE):
        r = apoio.indexar(docs[i:i + LOTE])
        if r.status_code != 202:
            apoio.falhou(f"/indexar respondeu {r.status_code}: {r.text[:200]}")
        print(f"{len(docs[i:i + LOTE])} trecho(s) enviados para a fila: {r.json()}")
    return len(docs)


if __name__ == "__main__":
    try:
        main(Path(sys.argv[1]) if len(sys.argv) > 1 else PASTA_PADRAO)
    except apoio.requests.RequestException as erro:
        sys.exit(f"falha ao falar com o gateway em {apoio.GATEWAY_URL}: {erro}")
