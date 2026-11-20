# Relatório de trade-offs: CAP, custo e latência

Este relatório trata das escolhas **desta** plataforma: gateway, inferência, vetorial,
worker, um Redis e o índice vetorial em arquivo. Ele cobre o item 7 do edital (item 10 de `TAREFAS.md`).
Os números locais foram medidos em 19/11/2026 com `scripts/medir_latencia.py 30` contra a stack do
`docker compose` (WSL2, 16 GB, LLM em modo simulado). A coluna do Render deve ser preenchida rodando o
mesmo script contra a URL pública depois do deploy.

## 1. Visão rápida da arquitetura

```
cliente --HTTPS--> gateway --HTTP--> inferencia --HTTP--> vetorial --disco--> indice.json
                     |                    \--HTTP--> LLM externo (só com LLM_API_KEY)
                     |--Redis: rate limit, resultado:<job_id>, fila:inferencia
                     v
                   worker --BLMOVE fila:inferencia--> inferencia /predict --> SETEX resultado:<job_id>
                   worker --BLMOVE fila:ingestao----> grava indice.json (escritor único)
```

O sistema tem dois caminhos:

- **Síncrono:** a pergunta RAG passa por gateway → inferência → vetorial (→ LLM).
- **Assíncrono:** a análise de sentimento passa por gateway → fila → worker → inferência,
  e o cliente consulta o resultado depois.

## 2. CAP

O teorema CAP diz que, durante uma **partição de rede**, um sistema distribuído escolhe
entre responder sempre (A) e responder só com dados consistentes (C). Abaixo está o que
cada parte deste sistema faz quando a partição acontece.

### 2.1 Redis único: sem réplica e sem eleição

Há **um** Redis, sem réplica nem Sentinel/Cluster. Não existe divergência entre cópias
porque só existe uma cópia. Quem alcança o Redis lê o último valor escrito, o que o
deixa perto de **CP**. O preço é que o Redis é um **ponto único de falha**: se ele cai ou
fica isolado, tudo o que depende dele fica indisponível. Não há réplica que assuma.

No Render, o Key Value gratuito **não persiste em disco**. Um reinício perde a fila e os
resultados. Localmente e no Kubernetes, o AOF (`appendonly yes`, `appendfsync everysec`)
limita a perda a cerca de 1 s de escritas.

### 2.2 O que acontece numa partição gateway ↔ Redis

| Função | Comportamento na partição | Escolha |
|---|---|---|
| **Rate limit** | O contador cai para um contador **em memória, por réplica** do gateway | **A** |
| **Enfileirar análise** (`POST` assíncrono) | Responde **503**. Não aceita um job que não consegue persistir | **C** |
| **Consultar resultado** | Responde **503** | **C** |
| **Pergunta RAG** (síncrona) | Continua funcionando, porque não usa o Redis | **A** |
| **Login / JWT** | Continua funcionando: o token é validado só com o segredo, sem estado | **A** |

O rate limit em memória faz o limite efetivo ficar `RATE_LIMIT_RPM × nº de réplicas`
enquanto a partição durar. Optamos por aceitar requisições a mais por alguns instantes
em vez de bloquear todos os usuários. Já recusar o enfileiramento é obrigatório: devolver
um `job_id` que nunca será processado seria pior que um 503 honesto.

### 2.3 Fila: disponibilidade com consistência eventual dos resultados

O `POST` assíncrono responde **202 + job_id** assim que o job é gravado no Redis. Ele não
espera a inferência terminar. Por isso, o pedido continua sendo aceito com a inferência
lenta, fora do ar ou com o disjuntor aberto (**A**). O resultado aparece **depois**: o
cliente vê `enfileirado` e só mais tarde `concluido` ou `falha` (**consistência eventual**).

A entrega é **pelo menos uma vez**. O `BLMOVE` move a mensagem para
`fila:inferencia:processando`, e ela só é removida (`LREM`) depois de o resultado ser
gravado. Se o worker morrer no meio, a mensagem é reprocessada. O mesmo job pode então
rodar duas vezes, o que não causa problema porque o resultado é gravado na chave
`resultado:<job_id>` (idempotente). Com várias réplicas de worker (KEDA), a recuperação
de pendentes pode reprocessar um job que outra réplica ainda está tratando. Esse caso
gera trabalho duplicado, mas não deixa resultado errado.

### 2.4 Índice vetorial: escritor único, leitura nunca corrompida

Só o worker de ingestão escreve `indice.json`. O vetorial apenas lê o arquivo. A gravação
é atômica: arquivo temporário, depois `fsync`, depois `os.replace`. Durante uma ingestão,
o leitor vê a **versão anterior completa** ou a **nova completa**, nunca um arquivo pela
metade. Um documento enviado em `POST /indexar` (202) só aparece nas buscas depois que o
worker processar o lote (**consistência eventual**, com leitura sempre íntegra). Como não
há trava, um segundo escritor quebraria essa garantia. Por isso o worker de ingestão tem
1 réplica e usa `Recreate` no Kubernetes (`k8s/worker.yaml`).

Medido localmente: o documento aparece na busca cerca de **66 ms** depois do `POST /indexar` (já incluindo o *polling*).

### 2.5 Resultados com TTL

`resultado:<job_id>` expira em `TTL_RESULTADOS_SEG` (padrão 3600 s). O Redis funciona
como armazenamento temporário de resultados, não como banco. Depois do TTL, consultar o
job retorna "não encontrado". Essa é uma escolha consciente: ela limita o uso de memória
(o Redis do plano gratuito é pequeno) e a retenção de dados (ver `docs/LGPD.md`).

## 3. Custo

### 3.1 Hoje: R$ 0

| Recurso | Plano | Limites que pesam no projeto |
|---|---|---|
| Web service do Render (gateway + inferência + vetorial + worker) | Gratuito | 512 MB de RAM, CPU compartilhada, **dorme após 15 min** sem requisição, **750 h/mês** de instância por conta, disco **efêmero** |
| Key Value do Render (Redis) | Gratuito | Memória pequena, **sem persistência**, acesso só pela rede privada |
| HTTPS | Incluso | Certificado automático em `*.onrender.com` |
| GitHub Actions + GHCR | Gratuito para repositório público | Minutos de CI limitados em repositório privado |
| LLM | Modo simulado | R$ 0. Com `LLM_API_KEY`, o custo passa a ser por token |

**Por que imagem única no Render.** Com um serviço por microsserviço, seriam 4 serviços
web. Eles dividiriam as 750 h/mês (4 × 720 h = 2.880 h, muito acima da cota), e o worker,
que nunca recebe requisição HTTP, dormiria e pararia a fila. Com os 4 processos numa
imagem (`deploy/render/`), um único serviço usa cerca de 720 h/mês e cabe na cota. O
isolamento continua entre **processos** (portas e falhas separadas; se um morre, o
container reinicia), mas deixa de existir entre **máquinas**. Localmente e no Kubernetes,
cada serviço tem seu próprio container.

**Efeitos colaterais do gratuito:**

- Os 4 processos Python dividem 512 MB.
  Medido com a imagem do Render rodando em `docker run --memory=512m`: **~282 MB** somados logo após
  o teste de ponta a ponta (gateway ~56 MB, inferência ~50 MB, worker ~85 MB, vetorial ~106 MB no compose).
- O disco é efêmero, então o `indice.json` some a cada reinício ou novo deploy e precisa
  ser semeado de novo.
- O Redis sem persistência perde jobs pendentes quando reinicia.

### 3.2 Próximo degrau (preços: verificar o valor atual em render.com/pricing)

| Necessidade | O que contratar | Efeito |
|---|---|---|
| Acabar com o cold start | Instância paga do Render (menor plano pago) | Não dorme; mais RAM e CPU |
| Índice persistente | Disco persistente (só existe em instância paga) ou banco vetorial gerenciado | Índice sobrevive a deploys |
| Fila durável | Key Value pago com persistência | Jobs sobrevivem a reinícios do Redis |
| Escalar serviços separadamente | Um serviço por microsserviço (worker como *background worker* pago) | Escala e isolamento por serviço; custo multiplicado pelo nº de serviços |
| Kubernetes (`k8s/`) | Cluster gerenciado + nós + load balancer + armazenamento RWX | Maior flexibilidade, maior custo fixo; geralmente o degrau mais caro |
| LLM real | API paga por token | Custo proporcional ao uso; as retentativas multiplicam esse custo (ver 4.3) |

Não citamos valores em reais porque os preços mudam. Antes de decidir, consulte a página
de preços de cada provedor.

## 4. Latência

### 4.1 Caminho síncrono x assíncrono

| Caminho | Saltos de rede até a resposta | Quem espera o processamento |
|---|---|---|
| `POST /perguntar` (RAG) | cliente→gateway, gateway→inferência, inferência→vetorial, (inferência→LLM) | O **cliente**, durante todos os saltos |
| `POST` assíncrono (sentimento) | cliente→gateway, gateway→Redis (gravar `resultado` + `RPUSH`, no mesmo MULTI/EXEC) | Ninguém. Resposta 202 imediata |
| `GET /resultados/{job_id}` | cliente→gateway, gateway→Redis | Ninguém. Só uma leitura |
| Processamento do job | worker→Redis (`BLMOVE`), worker→inferência, worker→Redis (`SETEX`, `LREM`) | Fora do tempo de resposta |

Cada salto HTTP interno soma serialização JSON, *spans* de rastreamento e a rede. No
Render, gateway, inferência e vetorial estão no mesmo container, então os saltos passam
por `127.0.0.1` e custam pouco. O Redis fica em outra máquina da rede privada.

| Métrica | Local (compose) | Render (medir após o deploy: `scripts/medir_latencia.py`) |
|---|---|---|
| `POST /login` (p50 / p95) | 1,9 / 5,7 ms | — |
| `POST /predict` síncrono (p50 / p95) | 3,4 / 4,8 ms | — |
| `POST /perguntar` (p50 / p95) | 8,3 / 11,2 ms | — |
| `POST /async/predict`, só a resposta 202 (p50 / p95) | 2,8 / 3,5 ms | — |
| Do 202 até `concluido` (p50 / p95) | 28,3 / 29,2 ms | — |
| Primeira requisição após o serviço dormir | (não se aplica) | ~1 min (documentado pelo Render) |

A latência do caminho assíncrono, do ponto de vista do usuário, é a espera na fila mais o
processamento mais o **intervalo de *polling*** do cliente. Ela é maior que a do
síncrono. Em troca, a resposta é previsível (202 imediato) e uma inferência lenta não
prende conexões no gateway.

### 4.2 Pior caso com timeouts, retentativas e disjuntor

Padrões do gateway para chamadas internas: **timeout de 5 s por tentativa, 3 tentativas,
espera progressiva de 0,3 s e 0,6 s**.

```
pior caso de uma chamada = 3 × 5 s (timeouts) + 0,3 s + 0,6 s (esperas) = 15,9 s
```

- **Disjuntor fechado, serviço travado:** o cliente espera até **~15,9 s** e recebe 503/504.
- **Disjuntor aberto:** depois de algumas falhas seguidas, o gateway responde **na hora**
  (milissegundos), sem chamar o serviço. O disjuntor serve para isso: troca 15,9 s de
  espera por uma falha rápida enquanto o serviço se recupera.
- **Cold start no Render (~1 min):** depois de 15 min sem tráfego, a primeira requisição
  espera o container subir e os 4 processos importarem as bibliotecas. O pior caso
  percebido é **~60 s + 15,9 s ≈ 76 s**. O monitor externo que chama `/saude` a cada
  5 min (ver `deploy/INSTRUCOES.md`) elimina esse caso, mas consome a cota de horas.
- **Caminho assíncrono:** o mesmo pior caso de 15,9 s acontece **dentro do worker** e não
  chega ao cliente. Ele só atrasa a mudança de `enfileirado` para `concluido`/`falha`.

**Orçamento de tempo aninhado.** A inferência chama o LLM com `LLM_TIMEOUT` (padrão 10 s) e
retentativas próprias: o pior caso dela é 3 × 10 s + 0,5 s + 1 s = **31,5 s**. A regra é que o tempo-limite
de dentro seja menor que o de fora; por isso o gateway usa um tempo-limite próprio para `/perguntar`
(`GATEWAY_TIMEOUT_PERGUNTAR`, padrão **40 s**) maior que esse pior caso. Se o gateway desistisse antes,
ele retentaria enquanto a inferência ainda trabalha (e gasta tokens) numa resposta que ninguém lê.
Quem aumentar `LLM_TIMEOUT` ou `LLM_TENTATIVAS` precisa aumentar `GATEWAY_TIMEOUT_PERGUNTAR` junto.

### 4.3 Amplificação de retentativas

As retentativas existem em duas camadas: gateway → inferência (3×) e inferência → LLM (3×).
Se as duas camadas repetissem o mesmo erro, uma pergunta poderia gerar **3 × 3 = 9 chamadas ao LLM**
justamente quando o provedor já está com problemas. Três decisões evitam isso:

1. quando o LLM falha, a inferência responde **503 com `"degradado": true`**, e o gateway repassa esse
   503 **sem retentar** e sem contar como falha no disjuntor (comprovado em `scripts/teste_resiliencia.py`:
   1 pergunta com o LLM quebrado gera exatamente 3 chamadas ao LLM, não 9);
2. o tempo-limite do gateway para `/perguntar` (40 s) é maior que o pior caso da inferência (31,5 s), então o
   gateway não desiste nem retenta enquanto a inferência ainda trabalha;
3. só erros transitórios (timeout, conexão, 5xx, 408, 429) são repetidos; 400/401/403/404 não
   (`llm.erro_transitorio`), e os disjuntores abrem após poucas falhas e cortam as chamadas.

### 4.4 Limite conhecido: TF-IDF recalculado a cada busca

O vetorial lê `indice.json` e reajusta o vocabulário TF-IDF com **todos** os documentos
para responder cada `/buscar`. O vocabulário precisa ser o mesmo que o worker usou ao
gravar os vetores. O custo é **O(n)** no número de documentos (leitura do JSON, ajuste e
similaridade com cada vetor), e a latência da busca cresce com o índice (veja a medição abaixo).
Com os poucos documentos de exemplo, o custo é desprezível.

Medido chamando a função de busca do vetorial direto, sem rede (média de 3 execuções):

| Documentos no índice | `/buscar` |
|---|---|
| 3 | 0,8 ms |
| 500 | 296 ms |
| 2000 | 7,6 s |

O crescimento é **pior que linear**. O vocabulário (n-gramas 1–2) cresce junto com o índice, e os
vetores são densos e guardados em JSON. Então cada busca lê e compara n vetores de dimensão que
também cresce. Para o tamanho do trabalho (dezenas de trechos) isso é irrelevante. Com milhares de
documentos, porém, estouraria o tempo-limite do gateway, e as mitigações abaixo passariam a ser obrigatórias.

Mitigações, em ordem de esforço:

1. Manter o vetorizador em memória e só reajustar quando o `mtime` do arquivo mudar.
2. Usar embeddings densos com vocabulário fixo, sem reajuste.
3. Usar um índice ANN ou banco vetorial (FAISS, pgvector, Qdrant), com busca sublinear.

## 5. Decisões e alternativas

| Decisão | Alternativa considerada | Por que ficamos com a decisão | Custo da decisão |
|---|---|---|---|
| Sentimento **assíncrono** (fila Redis + worker) | Chamada síncrona gateway → inferência | Gateway responde rápido; picos ficam na fila; escala de workers pela fila (KEDA) | Consistência eventual; cliente precisa fazer *polling* |
| RAG **síncrono** | Também via fila | Pergunta interativa; o usuário espera a resposta | Cliente sente toda a cadeia de latência e o pior caso de 15,9 s |
| **Redis único** para fila, resultados e rate limit | Redis com réplica/Sentinel; RabbitMQ/Kafka para a fila | Simples, cabe no plano gratuito, `BLMOVE` dá entrega pelo menos uma vez | Ponto único de falha; no Render, sem persistência |
| Entrega **pelo menos uma vez** + resultado idempotente | No máximo uma vez (`BRPOP` simples); exatamente uma vez | Nenhum job se perde se o worker cair | Job pode ser processado duas vezes |
| Rate limit no Redis, **com recurso em memória** | Falhar fechado (bloquear tudo se o Redis cair) | Disponibilidade do login e da API | Limite efetivo maior durante a falha |
| Índice em **arquivo JSON** com escritor único e gravação atômica | Banco vetorial (pgvector, Qdrant) | Zero dependências, didático, leitura sem trava | Busca O(n); volume compartilhado (RWX no Kubernetes); efêmero no Render |
| **Imagem única** no Render | Um serviço Render por microsserviço | Cabe em 750 h/mês; worker não dorme | Sem isolamento de máquina; 512 MB divididos por 4 processos |
| Render gratuito | Instância paga / Kubernetes gerenciado | Custo zero para um trabalho acadêmico | Cold start de ~1 min, disco e Redis efêmeros |
| Timeout + retentativa + disjuntor | Só timeout | Supera falhas transitórias e falha rápido em falhas longas | Pior caso de 15,9 s; amplificação de retentativas |
| JWT sem estado | Sessão no Redis | Validação sem I/O; funciona com o Redis fora | Não dá para revogar um token antes de `JWT_EXPIRACAO_MIN` |
| LLM **simulado** por padrão | Sempre LLM real | Funciona offline, sem custo e sem enviar dados a terceiros | Respostas não são geradas por um modelo de verdade |
