# Laboratório local — Kubernetes e NATS JetStream

Este ambiente mantém o Docker Compose com NATS Core e acrescenta um laboratório
local independente com Kind, Kubernetes e um cluster NATS JetStream de três nós.

## Arquitetura

O Kind cria um control plane e três workers. Cada worker recebe uma instância
NATS com armazenamento persistente. Pagamento, estoque e notificação começam
com duas réplicas e compartilham um durable pull consumer por etapa.

O stream `ORDERS` usa armazenamento em arquivo, três réplicas, retenção de uma
hora, ACK explícito, cinco tentativas máximas e janela de deduplicação de dez
minutos. O RPC continua usando request/reply do NATS Core.

## Pré-requisitos

Linux x86_64, Docker ativo, aproximadamente 12 GiB de RAM e 15 GiB livres.

Instale as ferramentas locais, sem `sudo`:

```bash
./k8s/scripts/install-tools.sh
```

Os binários ficam em `.tools/bin` e não são versionados.

Se algum `kube-proxy` falhar com `too many open files`, execute uma vez:

```bash
sudo sysctl -w \
  fs.inotify.max_user_instances=512 \
  fs.inotify.max_user_watches=524288
```

Essa alteração dura até o próximo reboot.

## Subir todo o laboratório

```bash
./k8s/scripts/local-up.sh
```

Verifique:

```bash
./k8s/scripts/status.sh
```

O resultado esperado é:

- quatro nós Kubernetes `Ready`;
- `nats-0`, `nats-1` e `nats-2` em workers diferentes;
- três PVCs `Bound`;
- duas réplicas prontas de pagamento, estoque e notificação;
- uma réplica pronta de RPC e dashboard.

## Abrir o dashboard

```bash
./k8s/scripts/dashboard-forward.sh
```

Acesse `http://127.0.0.1:5000`.

## Teste funcional

```bash
./k8s/scripts/smoke-test.sh
```

O script executa 100 pedidos com falha em caso de perda, testa RPC e lista os
durable consumers.

## Consultar o JetStream

```bash
NATS_BOX=$(./.tools/bin/kubectl get pod -n tpsd \
  -l app.kubernetes.io/component=nats-box \
  -o jsonpath='{.items[0].metadata.name}')

./.tools/bin/kubectl exec -n tpsd "$NATS_BOX" -- \
  nats --server nats://nats:4222 stream info ORDERS

./.tools/bin/kubectl exec -n tpsd "$NATS_BOX" -- \
  nats --server nats://nats:4222 consumer list ORDERS
```

## Derrubar um NATS durante a carga

```bash
./k8s/scripts/nats-failure-test.sh nats-0
```

Os outros dois servidores mantêm o quorum. O StatefulSet recria o pod removido
e ele sincroniza sua réplica novamente.

## Escalar workers

```bash
./.tools/bin/kubectl scale deployment payment -n tpsd --replicas=4
./.tools/bin/kubectl scale deployment stock -n tpsd --replicas=4
./.tools/bin/kubectl scale deployment notification -n tpsd --replicas=4
```

Retorne para duas réplicas substituindo `4` por `2`.

## Testar backlog

```bash
./.tools/bin/kubectl scale deployment notification -n tpsd --replicas=0
```

Aguarde todos os pods de notificação desaparecerem, publique pedidos pelo
dashboard e consulte `consumer info`. As mensagens aparecerão em `Unprocessed
Messages`. Depois restaure:

```bash
./.tools/bin/kubectl scale deployment notification -n tpsd --replicas=2
./.tools/bin/kubectl rollout status deployment/notification -n tpsd
```

O backlog deve voltar a zero.

## Simular queda de nó virtual

```bash
docker stop tpsd-worker2
./.tools/bin/kubectl get nodes,pods -n tpsd -w
```

Restaure o nó:

```bash
docker start tpsd-worker2
./.tools/bin/kubectl wait \
  --for=condition=Ready node/tpsd-worker2 \
  --timeout=180s
```

Os nós são contêineres no mesmo computador. Isso simula falha de nó, mas não
protege contra o desligamento físico do host.

## Encerrar

```bash
./k8s/scripts/cluster-down.sh
```

Esse comando exclui o cluster Kind e seus volumes locais. A implantação Docker
Compose e o código-fonte não são removidos.
