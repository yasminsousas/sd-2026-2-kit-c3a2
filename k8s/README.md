# Manifests de Kubernetes (extensão opcional)

Mesma arquitetura do `docker-compose.yml`, descrita para um cluster Kubernetes. O deploy
oficial do trabalho é o Render (ver `deploy/INSTRUCOES.md`); estes manifests são a
extensão opcional do edital e foram validados com `kubectl kustomize` e `kubeconform`
(esquemas do Kubernetes 1.31 + CRD do KEDA), sem rodar em cluster real.

| Arquivo | Conteúdo |
|---|---|
| `namespace.yaml` | Namespace `plataforma-ia` com Pod Security `restricted` |
| `configmap.yaml` | Configuração não sensível (URLs, TTLs, rate limit, OTLP) |
| `secret.yaml` | Segredo **de exemplo** (`JWT_SEGREDO`, `SENHA_PADRAO`, `LLM_API_KEY`) |
| `indice-pvc.yaml` | PVC do índice vetorial (`/dados/indice.json`) |
| `redis.yaml` | StatefulSet + PVC (AOF ligado, `noeviction`) + Service + NetworkPolicy |
| `jaeger.yaml` | Jaeger all-in-one (OTLP 4318, UI 16686, sem Ingress) |
| `gateway.yaml` | Deployment (2 réplicas) + Service + PDB + Ingress com TLS (cert-manager) |
| `inferencia.yaml` | Deployment (2 réplicas) + Service |
| `vetorial.yaml` | Deployment (2 réplicas) + Service, índice montado somente leitura |
| `worker.yaml` | `worker-ingestao` (1 réplica, escritor único) e `worker-inferencia` |
| `keda.yaml` | ScaledObject: escala `worker-inferencia` por `LLEN fila:inferencia` |
| `kustomization.yaml` | Junta tudo e define as tags das imagens |

Todos os pods rodam como não-root (UID 10001; Redis 999), sem escalonamento de
privilégio, com `capabilities: drop: [ALL]`, sistema de arquivos raiz somente leitura
(`/tmp` em `emptyDir`), probes e `requests`/`limits` definidos.

## Pré-requisitos

1. Imagens publicadas pelo CI em `ghcr.io/<owner>/plataforma-ia-<servico>:latest`
   (`gateway`, `inferencia`, `vetorial`, `worker`). Troque `<owner>` pelo usuário ou
   organização do GitHub **em minúsculas**, nos manifests ou com `kustomize edit set image`.
   Se o pacote do GHCR for privado, crie um `imagePullSecret`.
2. Ingress controller `ingress-nginx` e `cert-manager` instalados, com um ClusterIssuer:

   ```yaml
   apiVersion: cert-manager.io/v1
   kind: ClusterIssuer
   metadata:
     name: letsencrypt-prod
   spec:
     acme:
       server: https://acme-v02.api.letsencrypt.org/directory
       email: voce@example.com
       privateKeySecretRef:
         name: letsencrypt-prod
       solvers:
         - http01:
             ingress:
               ingressClassName: nginx
   ```

   Troque `plataforma-ia.example.com` em `gateway.yaml` por um domínio seu.
3. KEDA instalado (para `keda.yaml`); sem ele, retire `keda.yaml` do `kustomization.yaml`
   e o `worker-inferencia` fica fixo em 1 réplica.
4. Uma StorageClass com suporte a `ReadWriteMany` para o índice (ver abaixo).

## Como aplicar

```bash
# 1. Segredo real (não use os placeholders de secret.yaml)
kubectl create namespace plataforma-ia
kubectl -n plataforma-ia create secret generic plataforma-ia-segredos \
  --from-literal=JWT_SEGREDO="$(openssl rand -hex 32)" \
  --from-literal=SENHA_PADRAO='<senha-forte>' \
  --from-literal=LLM_API_KEY=''
# ... e retire secret.yaml do kustomization.yaml

# 2. Todo o resto, de uma vez (o kustomize ordena namespace/config/PVC antes dos pods)
kubectl apply -k k8s/

# 3. Acompanhar
kubectl -n plataforma-ia get pods,svc,ingress,scaledobject
kubectl -n plataforma-ia port-forward svc/jaeger 16686:16686   # UI do Jaeger
```

Sem kustomize, a ordem com `-f` é: `namespace` → `configmap` → `secret` → `indice-pvc`
→ `redis` → `jaeger` → `inferencia` → `vetorial` → `worker` → `gateway` → `keda`.

Para semear o índice, use a API pública: `POST /indexar` no gateway (ou `port-forward`
no `vetorial`).

## Volume do índice: ReadWriteOnce x ReadWriteMany

O `worker-ingestao` grava `/dados/indice.json` e o `vetorial` o lê direto do disco.
Os dois precisam montar o **mesmo** volume, e aí entra o modo de acesso do PVC:

- **ReadWriteOnce (RWO)** — o volume é anexado a **um nó** por vez (a restrição é por nó,
  não por pod). Funciona só se o worker e todas as réplicas do vetorial caírem no mesmo
  nó; se o agendador puser um pod em outro nó, ele fica preso em `ContainerCreating`
  com erro `Multi-Attach`. É o único modo dos discos em bloco comuns (EBS, Persistent
  Disk, Azure Disk) e do `local-path` do kind/k3s.
- **ReadWriteMany (RWX)** — vários nós montam ao mesmo tempo. Exige armazenamento de
  arquivos em rede (NFS, CephFS, AWS EFS, Azure Files, GCP Filestore).

**Solução adotada:** o PVC pede `ReadWriteMany`, para que o vetorial possa ter várias
réplicas espalhadas por nós. A segurança da leitura concorrente não depende do volume,
e sim do código: só existe **um escritor** (`worker-ingestao` com `replicas: 1` e
`strategy: Recreate`, que impede dois pods escritores durante um deploy), o vetorial
monta o volume `readOnly`, e a gravação é atômica (arquivo temporário + `fsync` +
`os.replace`). O leitor vê a versão anterior ou a nova, nunca um arquivo pela metade
(consistência eventual, ver `docs/RELATORIO_TRADEOFFS.md`).

**Cluster de um nó** (minikube, kind, k3s de laboratório): troque `accessModes` em
`indice-pvc.yaml` para `ReadWriteOnce`. Como todos os pods ficam no mesmo nó, o RWO
atende. Em cluster com vários nós e sem RWX, a alternativa seria forçar o vetorial no
nó do worker com `podAffinity`, perdendo a distribuição das réplicas entre nós.

Evolução natural: tirar o índice do sistema de arquivos e usar um banco vetorial em
rede (pgvector, Qdrant). Assim, o vetorial deixaria de depender de volume compartilhado.

## Autoescalonamento (KEDA)

`keda.yaml` cria um HPA gerenciado pelo KEDA que lê `LLEN fila:inferencia` a cada 10 s:
réplicas = `ceil(tamanho da fila / 5)`, entre 1 e 5. Para ver funcionando, enfileire
várias análises (`POST` assíncrono no gateway) e acompanhe com
`kubectl -n plataforma-ia get hpa,pods -w`. O `worker-ingestao` não escala, porque é o
escritor único do índice.
