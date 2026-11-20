# Deploy em nuvem (Render, plano gratis)

## Como fica na nuvem

```
internet --HTTPS--> Render (plataforma-ia)                       Render Key Value
                    ┌───────────────────────────────────────┐    (Redis gratis)
                    │ gateway   :$PORT  (unico exposto)     │──fila──┐
                    │ inferencia 127.0.0.1:8001             │        │
                    │ worker     (saude 127.0.0.1:8002)     │<───────┘
                    └───────────────────────────────────────┘
```

- **Localmente** (`docker compose up --build`): cada microsservico roda no seu container.
- **No Render**: os mesmos 3 servicos rodam como **processos separados** numa imagem unica
  (`deploy/render/Dockerfile` + `deploy/render/start.sh`), e o Redis e o Key Value gerenciado.

Motivo: no plano gratis, cada servico web **dorme apos 15 min sem requisicoes** e a conta tem
**750 h/mes** no total. Tres servicos separados nao caberiam nas horas, e o worker (que nunca
recebe requisicoes) dormiria e a fila pararia. Com uma imagem so, um unico servico usa ~720 h/mes.

HTTPS: o Render entrega o certificado automaticamente em `https://<nome>.onrender.com`.

## Passo a passo

1. Crie conta em https://render.com (pode entrar com o GitHub; nao pede cartao).
2. **New > Blueprint** e selecione este repositorio. O Render le o `render.yaml` da raiz e cria:
   - `plataforma-ia` (Web Service, Docker, free)
   - `plataforma-ia-redis` (Key Value, free)
3. Ele vai pedir o valor de `SENHA_PADRAO` (senha do usuario `admin` do `/login`).
   Escolha uma senha forte. O `JWT_SEGREDO` e gerado sozinho pelo Render.
4. Clique em **Apply** e espere o primeiro deploy (alguns minutos).
5. Copie a URL publica (ex.: `https://plataforma-ia.onrender.com`) e coloque no README.

A cada `git push` na `main`, o Render publica de novo sozinho (`autoDeploy: true`).

## Manter no ar (evitar que durma)

Sem requisicoes por 15 min o servico dorme, e a proxima requisicao demora ~1 min.
Para manter acordado, crie um monitor gratis que chama `/saude` a cada 5 minutos:

1. Crie conta em https://uptimerobot.com (gratis).
2. **Add New Monitor** > tipo **HTTP(s)** > URL `https://<sua-url>.onrender.com/saude`
   > intervalo **5 minutes** > **Create Monitor**.

Isso tambem gera um historico de disponibilidade que pode ir para o relatorio.
Atencao: as 750 h/mes sao por conta. Nao deixe outros servicos gratis ligados na mesma conta.

## Testar a URL publica

```bash
URL=https://plataforma-ia.onrender.com

curl $URL/saude

TOKEN=$(curl -s -X POST $URL/login -H 'Content-Type: application/json' \
  -d '{"usuario":"admin","senha":"SUA_SENHA"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

curl -X POST $URL/predict -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"texto":"servico excelente"}'

curl -X POST $URL/async/predict -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"texto":"atendimento pessimo"}'
# use o job_id devolvido:
curl $URL/resultados/<job_id> -H "Authorization: Bearer $TOKEN"
```

## Limitacoes do plano gratis (vale citar no relatorio de trade-offs)

- Dorme apos 15 min sem trafego; o monitor acima evita isso.
- O Key Value gratis pode ser reiniciado pelo Render a qualquer momento e **perde os dados**.
  Como a fila e os resultados sao temporarios (TTL de 1 h), isso so afeta tarefas em andamento.
- 512 MB de RAM e CPU compartilhada para os 3 processos.
