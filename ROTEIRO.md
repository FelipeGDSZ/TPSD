# Roteiro de implantação e apresentação

Este projeto usa NATS Core. Não há persistência, redelivery, ACK explícito ou
garantia de processamento após uma interrupção. A entrega é at-most-once.

## Topologia recomendada

- VM 1: servidor NATS e dashboard.
- VM 2: pagamento, estoque, notificação e RPC.
- Comunicação entre as VMs: IPs privados da VCN.
- Porta 4222: permitida somente da VM de workers para a VM NATS.
- Porta 8222: não deve ser publicada na internet; use túnel SSH.
- Porta 5000: restrinja ao seu IP durante a apresentação.
- Porta 22: restrinja ao seu IP.

Os manifests estão em `deploy/nats-dashboard.compose.yml` e
`deploy/workers.compose.yml`.

## Checklist antes de criar as VMs

1. Confirme que a branch está limpa e que todos os commits foram enviados.
2. Crie duas VMs na mesma VCN/sub-rede, anotando os IPs privados.
3. Confirme a arquitetura com `uname -m`; as imagens usadas aceitam ARM64.
4. Instale Docker Engine e o plugin Docker Compose nas duas VMs.
5. Clone o mesmo commit do projeto nas duas máquinas.
6. Configure as regras de rede da VCN e o firewall do sistema operacional.
7. Não abra 4222 ou 8222 para `0.0.0.0/0`.

## VM 1: NATS e dashboard

Crie o arquivo local de ambiente:

```bash
cp deploy/.env.example deploy/.env
```

Edite `deploy/.env` e defina `NATS_BIND_IP` com o IP privado da VM 1.
Depois execute:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml up --build -d
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml ps
```

O dashboard ficará na porta 5000. Para consultar o monitor NATS sem expor
a porta 8222, abra no computador local:

```bash
ssh -L 8222:127.0.0.1:8222 usuario@IP_PUBLICO_VM_1
```

Então acesse `http://localhost:8222`.

## VM 2: workers

Crie `deploy/.env` e ajuste `NATS_URL` para o IP privado da VM 1:

```dotenv
NATS_URL=nats://IP_PRIVADO_VM_1:4222
```

Suba os serviços:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up --build -d
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Todos os containers devem ficar `healthy`. Se não ficarem, valide primeiro
a regra de entrada TCP 4222 na VM 1 e a rota privada entre as VMs.

## Teste ponta a ponta

Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python producer.py --total 10 --report 1
```

Na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml logs -f \
  payment stock notification
```

O mesmo `order_id` deve passar por pagamento, estoque e notificação.

Teste do RPC a partir do dashboard:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python rpc_client.py
```

## Teste de rajada e linha de base

Com uma réplica de cada worker na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=1 --scale stock=1 --scale notification=1
```

Na VM 1, envie 500 pedidos sem atraso artificial:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 500 --timeout 180 --label 1-worker
```

O benchmark assina `order.notify.confirm` antes da rajada e correlaciona
somente os pedidos desse teste. Apresente mensagens enviadas, eventos que
chegaram à etapa final, perdas, throughput E2E e latências p50/p95/p99.

## Teste de escala horizontal

Escale todas as etapas; escalar apenas pagamento deslocaria o gargalo para
estoque ou notificação:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=2 --scale stock=2 --scale notification=2
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Espere todos ficarem `healthy` e repita exatamente a mesma carga:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 500 --timeout 180 --label 2-workers
```

Compare o throughput e as latências. Os Queue Groups distribuem cada evento
entre as réplicas da mesma etapa; eles não criam cópias da mensagem.

Resultado de referência obtido nas duas VMs `E2.1.Micro`:

- 1 worker: 32 pedidos/s, p95 de 14,55 s e zero perdas.
- 2 workers: 51 pedidos/s, p95 de 9,29 s e zero perdas.
- Ganho: 59% no throughput e redução de 36% na latência p95.

## Queda de um container durante a carga

Mantenha duas réplicas e inicie uma carga maior na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 1000 --timeout 180 --label falha-payment
```

Enquanto o teste estiver rodando, liste as réplicas na VM 2:

```bash
docker ps --filter label=com.docker.compose.project=tpsd-workers \
  --filter label=com.docker.compose.service=payment
```

Copie o ID de uma réplica de pagamento e pare somente ela:

```bash
docker kill --signal KILL ID_DA_REPLICA
```

A outra réplica continua processando. Ao final, é possível que o
benchmark mostre perdas: se o container caiu depois de receber eventos e antes
de publicar a próxima etapa, NATS Core não faz redelivery. Isso demonstra
continuidade parcial do serviço, não entrega garantida.

Restaure as duas réplicas:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d --scale payment=2
```

## Reinício do servidor NATS

Fora do teste de carga, reinicie o NATS e acompanhe a reconexão:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml restart nats
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml logs -f
```

Os clientes se reconectam, mas o servidor NATS único é um ponto de falha e
mensagens publicadas durante a indisponibilidade podem ser perdidas.

## Diagnóstico e encerramento

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml logs --tail=100
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml logs --tail=100
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml down
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml down
```

Não use `down -v`: este projeto não precisa de volumes persistentes.
Mantenha o Compose local como plano B para a apresentação.
