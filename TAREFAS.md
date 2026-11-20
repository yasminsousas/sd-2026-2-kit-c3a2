# TAREFAS - Trabalho C3.A2 (Plataforma de IA como Servico)

Projeto integrador: reaproveita o C2.A2 (RAG distribuido, indice vetorial, worker confiavel) e o
servico de inferencia + worker de fila do C3.A2. Detalhes de cada item no README.

## Nucleo obrigatorio

- [x] **1. Autenticacao JWT no gateway** - `servicos/gateway/autenticacao.py`, `app.py`
      `POST /login` emite o token; demais rotas respondem 401 sem token valido.
- [x] **2. Rate limit no gateway** - `servicos/gateway/limite.py`
      Por usuario (30/min) e por IP no login (10/min), contado no Redis, com fallback em memoria.
- [x] **3. Dockerfile de cada servico** - `servicos/<servico>/Dockerfile` (gateway, inferencia, vetorial, worker)
- [x] **4. docker-compose completo** - `docker compose up --build -d` sobe tudo (+ Redis e Jaeger)
- [ ] **5. Deploy em nuvem (free tier)** - `render.yaml` + `deploy/render/` prontos e testados localmente
      (imagem com 512 MB). Falta aplicar o Blueprint no Render e confirmar a URL publica no README.
- [x] **6. Resiliencia** - tempo-limite, retentativa com backoff exponencial e circuit breaker em
      gateway, worker e inferencia (`scripts/teste_resiliencia.py --docker` prova com falhas reais)
- [x] **7. Observabilidade** - OpenTelemetry + Jaeger; o mesmo trace cobre gateway -> fila -> worker -> inferencia
- [ ] **8. HTTPS** no servico publicado - automatico no Render (`*.onrender.com`) + HSTS no gateway;
      confirmar apos o deploy.
- [x] **9. Secao de conformidade no README** - README "Conformidade com a LGPD" + `docs/LGPD.md`
- [x] **10. Relatorio de trade-offs** - `docs/RELATORIO_TRADEOFFS.md` (CAP, custo e latencia, com medicoes)

## Extensoes opcionais

- [x] Autoescalonamento de workers conforme o tamanho da fila (`k8s/keda.yaml`)
- [ ] Componente serverless ou na borda
- [x] CI/CD publicando automaticamente a cada commit (`.github/workflows/ci.yml` + `autoDeploy` do Render)
- [ ] Painel de metricas (parcial: `GET /saude/detalhada` e Jaeger)
- [x] Manifests de Kubernetes (`k8s/`)

## Antes de entregar

- [x] Clone do zero e rode o comando unico - `docker compose up --build -d` + `scripts/teste_ponta_a_ponta.py`
- [ ] A URL publica esta no ar? (`GATEWAY_URL=https://... python scripts/teste_ponta_a_ponta.py`)
- [x] Toda configuracao esta em variavel de ambiente (nada fixo no codigo)?
- [x] Os logs estao livres de dados pessoais? (conferido nos logs dos containers)
