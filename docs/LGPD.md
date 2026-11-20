# Conformidade com a LGPD (Lei 13.709/2018)

Este documento mapeia quais dados pessoais a plataforma trata, com qual finalidade, por
quanto tempo e com quais proteções. Ele cobre o item 9 de `TAREFAS.md`. É um mapeamento
técnico de um trabalho acadêmico, **não um parecer jurídico**: as bases legais abaixo são
**sugestões** e precisariam ser validadas por um encarregado (DPO) ou assessoria jurídica
antes de um uso real.

## 1. Princípio adotado: guardar o mínimo, pelo menor tempo

- Nada é guardado "por via das dúvidas". Cada dado tem uma finalidade e um prazo.
- Onde precisamos identificar o usuário, usamos um **hash** do identificador, nunca o nome.
- **Logs e traces não carregam conteúdo**: nunca registram texto analisado, pergunta,
  senha ou token. Registram só `trace_id`, `job_id`, status, tamanhos e latência.
- Sem `LLM_API_KEY`, o LLM roda em **modo simulado** e nenhum dado sai da plataforma.

## 2. Dados tratados

| Dado | Onde aparece | Finalidade | Retenção | Base legal sugerida |
|---|---|---|---|---|
| **Credenciais de login** (usuário e senha) | Corpo do `POST /login` | Autenticar | **Não armazenadas.** São comparadas em memória com a configuração e descartadas | Execução de contrato (art. 7º, V) |
| **Token JWT** | Cabeçalho `Authorization` | Autorizar as chamadas seguintes | **Não armazenado no servidor.** Expira em `JWT_EXPIRACAO_MIN` (padrão 60 min) | Execução de contrato (art. 7º, V) |
| **Texto enviado para análise de sentimento** | `fila:inferencia` e, durante o processamento, `fila:inferencia:processando` (Redis) | Executar a análise pedida | Até o worker processar o job (normalmente segundos). Depois é removido da fila | Execução de contrato (art. 7º, V) |
| **Resultado da análise** (sentimento, confiança) | `resultado:<job_id>` (Redis) | Devolver o resultado ao usuário | **TTL de 1 h** (`TTL_RESULTADOS_SEG=3600`) ou até `DELETE /resultados/{job_id}` | Execução de contrato (art. 7º, V) |
| **Pergunta RAG** | Memória dos serviços durante a requisição | Responder a pergunta | **Não armazenada.** Existe só durante a requisição | Execução de contrato (art. 7º, V) |
| **Documentos indexados** | `fila:ingestao` e depois `indice.json` (volume do vetorial) | Base de conhecimento consultada pelo RAG | **Até serem removidos do índice.** Persistem entre reinícios (exceto no disco efêmero do Render) | Legítimo interesse (art. 7º, IX), desde que **não contenham dados pessoais** |
| **Identificador do usuário (hash)** | Campo `dono` do job e chave `ratelimit:<hash>:<minuto>` | Permitir que só o dono leia ou apague o próprio resultado; limitar requisições | `dono`: o TTL do resultado (1 h). Rate limit: **60 s** | Execução de contrato (art. 7º, V) e legítimo interesse para segurança (art. 7º, IX) |
| **Endereço IP** | Memória do gateway / Redis, no rate limit do `/login` | Mitigar força bruta no login | **Só TTL curto** (janela do rate limit). Não é gravado em log da aplicação | Legítimo interesse, prevenção a fraude e segurança (art. 7º, IX) |
| **Logs e traces** (Jaeger) | Saída padrão dos containers; Jaeger | Operar, depurar e medir | Logs: retenção da plataforma de hospedagem. Jaeger local: em memória, some ao reiniciar | Legítimo interesse (art. 7º, IX) |

### Observações importantes

- **Hash não é anonimização.** O SHA-256 truncado do identificador é **pseudonimização**:
  quem conhece a lista de usuários pode recalcular o hash. Para a LGPD, o dado continua
  sendo pessoal (art. 12 e art. 13, §4º) e segue as mesmas regras de retenção. A
  vantagem real é não espalhar o identificador em claro pelo Redis.
- **Texto livre pode conter dado pessoal.** O usuário pode digitar nome, CPF ou um dado
  sensível no texto ou na pergunta. A plataforma não inspeciona o conteúdo. Ela limita o
  risco com retenção curta e com logs sem conteúdo.
- **Não indexe dados pessoais.** O índice vetorial é a única base que persiste sem TTL e
  é consultada por todos os usuários: qualquer trecho indexado pode aparecer como
  contexto na resposta de outra pessoa. Use só documentos públicos ou institucionais.
- **Mensagens inválidas.** Mensagens que o worker não consegue interpretar vão para
  `fila:inferencia:erros` e `fila:ingestao:erros`, para análise. Elas podem conter o
  texto original, então o worker as **limita às últimas 100 mensagens** (`FILA_ERROS_MAX`, via `LTRIM`)
  e as faz **expirar em 7 dias** (`FILA_ERROS_TTL_SEG=604800`), na mesma transação que grava a mensagem.

## 3. Direitos do titular (art. 18)

| Direito | Como é atendido |
|---|---|
| Confirmação e acesso | `GET /resultados/{job_id}` devolve o que está guardado sobre aquele job (só para o dono) |
| Eliminação | `DELETE /resultados/{job_id}` apaga o resultado na hora (só o dono). Se o job ainda estiver na fila, o worker vê que o recibo sumiu e **descarta** o resultado em vez de recriá-lo. Sem o pedido, ele expira sozinho em 1 h |
| Informação sobre compartilhamento | Este documento, seção 5 |
| Correção / portabilidade | Não se aplicam na prática: a plataforma não mantém cadastro nem histórico do usuário |
| Eliminação de documento indexado | Hoje é manual (remover do índice e reindexar). Ver seção 6 |

## 4. Segurança (art. 46)

- **HTTPS** em todo acesso público: certificado automático do Render; no Kubernetes,
  cert-manager com redirecionamento forçado para HTTPS (`k8s/gateway.yaml`).
- **Autenticação JWT** em todas as rotas, exceto `/login` e `/saude`. O token é assinado
  com `JWT_SEGREDO` e tem validade curta.
- **Segredos fora do código:** `JWT_SEGREDO`, `SENHA_PADRAO` e `LLM_API_KEY` vêm de
  variáveis de ambiente. No Render, o `JWT_SEGREDO` é gerado pela plataforma e a senha
  é digitada no painel. No Kubernetes, ficam num Secret.
- **Redis sem exposição pública:** no Render, o Key Value é acessível só pela rede
  privada (`ipAllowList: []`). No compose, o Redis não publica porta. No Kubernetes, o
  Service é `ClusterIP` e há uma NetworkPolicy.
- **Único ponto de entrada:** só o gateway é público. Inferência, vetorial, worker e
  Jaeger ficam na rede interna.
- **Rate limit** por usuário e por IP no login (força bruta) reduz abuso.
- **Containers não-root**, sem privilégios extras.
- **Logs sem conteúdo** e log de acesso do uvicorn desligado, para não gravar IP e caminho
  de cada requisição.

## 5. Compartilhamento com terceiros

| Situação | O que sai da plataforma | Para quem |
|---|---|---|
| **Modo simulado** (sem `LLM_API_KEY`, o padrão) | **Nada** | Ninguém |
| **LLM real** (`LLM_API_KEY` configurada) | A **pergunta RAG** e os **trechos de documentos** recuperados como contexto | Provedor do LLM em `LLM_API_URL` (por padrão, um provedor estrangeiro) |
| Hospedagem | Todos os dados em trânsito e em repouso na infraestrutura | Render (operador), com servidores fora do Brasil (região `oregon`) |

Ao ligar o LLM real, há **transferência internacional** de dados (arts. 33 a 36). Nesse
caso é preciso:

- informar o usuário antes do uso;
- verificar os termos do provedor sobre retenção e uso dos dados para treinamento;
- reforçar a regra de não indexar dados pessoais, já que os trechos vão junto com a
  pergunta.

O texto da análise de sentimento **não** vai para o LLM: o modelo de sentimento roda
localmente no serviço de inferência.

## 6. O que falta para produção real

- **Bases legais e aviso de privacidade** validados juridicamente, com o encarregado
  (DPO) nomeado e um canal para o titular (art. 41).
- **Gestão de usuários de verdade:** hoje há um único usuário configurado por variável de
  ambiente. Em produção seriam necessários cadastro, senha com hash forte (bcrypt/argon2)
  e revogação de token.
- **Remoção de documento do índice por API** (hoje só é possível reindexando), com
  registro de quem pediu.
- **Retenção dos logs** da hospedagem configurada e documentada, e **Jaeger persistente**
  com prazo de retenção definido.
- **Redis com autenticação e TLS** (`rediss://`, senha/ACL), mesmo em rede privada.
- **Limite e expiração das filas de erro.**
- **Contrato de operador** com o provedor de LLM e com a hospedagem; avaliação de região
  no Brasil se necessário.
- **Registro das operações de tratamento** (art. 37) e **relatório de impacto** (RIPD) se
  houver dado sensível.
- **Plano de resposta a incidentes** com comunicação à ANPD e aos titulares (art. 48).
