"""
Indice vetorial persistente, compartilhado pelo worker (escreve) e pelo servico
vetorial (so le). Porte de C2.A2 servicos/ingestao/armazenamento.py.

O indice fica em um arquivo JSON dentro de DATA_DIR (no compose e no Kubernetes,
um volume montado nos dois servicos). A escrita e atomica (grava em arquivo
temporario e troca com os.replace), entao quem le o arquivo nunca enxerga um
indice pela metade - nem se o processo cair no meio da gravacao. E isso que
permite ao servico vetorial ler o disco direto, sem trava e sem chamada HTTP.

Regra de consistencia: o vocabulario TF-IDF e reajustado com TODOS os textos do
indice a cada atualizacao e todos os vetores sao recalculados. Assim os vetores
guardados sempre pertencem ao mesmo vocabulario, e o servico vetorial consegue
reproduzi-lo ajustando um Vetorizador com os mesmos textos.

Apenas UM processo deve escrever (o consumidor de ingestao do worker): nao ha
trava de arquivo.
"""
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

from servicos.comum.embeddings import Vetorizador

DATA_DIR = Path(os.getenv("DATA_DIR", "dados"))
ARQUIVO = DATA_DIR / "indice.json"


def texto_indexavel(texto: str) -> bool:
    """
    True se o texto tem ao menos um "token" para o TF-IDF (2+ caracteres de palavra).
    Um texto como "!!!" nao gera vocabulario: se for o unico do lote o vetorizador
    levanta "empty vocabulary" e a mensagem seria reprocessada para sempre.
    """
    return re.search(r"\w\w+", texto) is not None


def _identificador(texto: str, origem: str) -> str:
    """
    Identifica um documento pelo conteudo: reenviar o mesmo nao duplica.
    O par e serializado como JSON para nao haver ambiguidade entre os campos
    (com um separador simples, origem "x\\ny"+texto "z" colidiria com "x"+"y\\nz").
    """
    par = json.dumps([origem, texto], ensure_ascii=False)
    return hashlib.sha1(par.encode("utf-8")).hexdigest()


def carregar() -> list:
    """
    Le o indice do disco. Devolve lista vazia se ainda nao existir.
    Arquivo corrompido levanta ValueError: quem chama decide (o vetorial responde 503).
    """
    try:
        with open(ARQUIVO, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def _salvar(itens: list) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fd, caminho_tmp = tempfile.mkstemp(dir=DATA_DIR, suffix=".tmp")
    try:
        # mkstemp cria com permissao 0600; o servico vetorial le o mesmo volume e
        # pode rodar com outro usuario, entao o indice precisa ser legivel por todos
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(itens, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(caminho_tmp, ARQUIVO)
    except BaseException:
        if os.path.exists(caminho_tmp):
            os.unlink(caminho_tmp)
        raise


def adicionar(docs: list) -> dict:
    """
    Acrescenta documentos ({"texto", "origem"}) ao indice, reindexa tudo e grava.
    Documentos repetidos (mesmo texto e mesma origem) sao ignorados.
    """
    itens = carregar()
    # ids recalculados a partir do conteudo: indices gravados com a versao antiga do
    # identificador continuam reconhecidos e sao regravados com o id novo
    existentes = {_identificador(i["texto"], i["origem"]) for i in itens}
    novos = 0
    for d in docs:
        ident = _identificador(d["texto"], d["origem"])
        if ident in existentes:
            continue
        existentes.add(ident)
        itens.append({"id": ident, "texto": d["texto"], "origem": d["origem"]})
        novos += 1

    if novos:
        for item in itens:
            item["id"] = _identificador(item["texto"], item["origem"])
        vetorizador = Vetorizador()
        vetorizador.ajustar([i["texto"] for i in itens])
        for item in itens:
            item["vetor"] = vetorizador.vetorizar(item["texto"])
        _salvar(itens)

    return {"novos": novos, "total": len(itens)}
