# Roteiro da apresentação — NATS Core e Sistemas Distribuídos

O projeto usa duas VMs Oracle Cloud:

- **VM 1:** NATS Core e dashboard.
- **VM 2:** pagamento, estoque, notificação e servidor RPC.

A entrega é **at-most-once**. Não há JetStream, persistência, ACK explícito ou
redelivery. Tempo sugerido: 12 a 15 minutos.

## O que demonstrar

1. Comunicação entre processos em VMs diferentes.
2. Coreografia assíncrona por eventos.
3. Request/reply por RPC.
4. Rajada com uma réplica de cada worker.
5. Escala horizontal com duas réplicas.
6. Queda de um contêiner durante o processamento.
7. Limites assumidos do NATS Core.

## Preparação antes da aula

Na VM 1:

```bash
cd ~/TPSD
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml up --build -d
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml ps
```

Na VM 2, comece com uma réplica por etapa:

```bash
cd ~/TPSD
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up --build --force-recreate -d \
  --scale payment=1 --scale stock=1 --scale notification=1
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Espere todos ficarem `healthy`. O número em nomes como `payment-2` é
apenas um identificador do Docker, não a quantidade de réplicas.

Deixe preparados:

- dashboard aberto em `http://IP_PUBLICO_VM_1:5000`;
- um terminal conectado à VM 1;
- um terminal conectado à VM 2;
- porta 5000 restrita ao IP da apresentação;
- porta 4222 liberada apenas entre as VMs pela rede privada.

## 1. Apresentar a arquitetura

```text
                           VM 1
                    +----------------+
Usuário/benchmark ->| Dashboard      |
                    | NATS Core      |
                    +-------+--------+
                            |
                 rede privada, porta 4222
                            |
                           VM 2
        +-------------------+--------------------+
        |                   |                    |
        v                   v                    v
   Pagamento  -------->  Estoque  -------->  Notificação
order.payment.new   order.stock.reserve   order.notify.confirm

Dashboard/RPC client -- inventory.check --> RPC server
```

Fala sugerida:

> O NATS é o middleware de mensagens. Os serviços não chamam diretamente o
> endereço uns dos outros: publicam e assinam subjects. Isso reduz o acoplamento
> e permite trocar ou replicar consumidores sem alterar o produtor.

A cadeia principal é uma **coreografia**: cada serviço reage a um evento e
publica o próximo. Não existe um orquestrador central controlando as etapas.

## 2. Confirmar a distribuição

Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml ps
```

Na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Mostre que NATS/dashboard e workers estão em máquinas diferentes e se comunicam
pela rede. Isso caracteriza distribuição física, não apenas módulos separados
dentro de um processo.

## 3. Demonstrar um pedido ponta a ponta

Na VM 2, acompanhe os logs:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml logs -f --tail=0 \
  payment stock notification
```

Na VM 1, envie um pedido:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python producer.py --total 1 --report 1
```

Mostre o mesmo `order_id` passando por:

1. `order.payment.new` — pagamento aprova;
2. `order.stock.reserve` — estoque reserva;
3. `order.notify.confirm` — notificação encerra o ciclo.

Pressione `Ctrl+C` somente no terminal dos logs.

Relação com middleware: o produtor conhece apenas o subject inicial; pagamento
não conhece a localização do estoque; estoque não conhece a localização da
notificação. O NATS realiza a entrega e o desacoplamento espacial.

## 4. Mostrar o dashboard

Destaque:

- conexões NATS ativas;
- workers de pagamento, estoque e notificação;
- taxas de publicação e entrega;
- estado de cada etapa.

O dashboard consulta a API de monitoramento do NATS. A quantidade de workers
representa subscriptions ativas, não o número no nome do contêiner.

## 5. Demonstrar request/reply

Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python rpc_client.py
```

Fala sugerida:

> Além do pub/sub assíncrono, o middleware oferece request/reply. O cliente
> consulta `inventory.check`, o NATS encaminha a solicitação e cria a rota de
> resposta para o servidor RPC devolver o resultado.

O RPC demonstra comunicação com resposta; o fluxo de pedidos demonstra eventos
assíncronos.

## 6. Linha de base com um worker

Na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=1 --scale stock=1 --scale notification=1
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Espere todos ficarem `healthy` e feche logs contínuos para não consumir CPU.
Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 500 --timeout 180 --label apresentacao-1-worker
```

Explique:

- **PUBLICAÇÃO:** velocidade de entrada no NATS, não de conclusão;
- **E2E:** pedidos que atravessaram as três etapas por segundo;
- **p50:** metade dos pedidos terminou até esse tempo;
- **p95/p99:** comportamento dos pedidos mais lentos;
- **PERDIDOS:** pedidos sem evento final dentro do timeout.

Resultado de referência:

- 500 enviados e 500 concluídos;
- zero perdas;
- 32 pedidos/s E2E;
- p95 de 14,55 s.

## 7. Escala horizontal

Na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=2 --scale stock=2 --scale notification=2
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

Espere as seis réplicas ficarem `healthy` e confirme dois workers por etapa no
dashboard. Na VM 1, repita a mesma carga:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 500 --timeout 180 --label apresentacao-2-workers
```

Resultado de referência estabilizado:

- 500 enviados e 500 concluídos;
- zero perdas;
- 58 pedidos/s E2E;
- p95 de 8,21 s;
- ganho aproximado de 81% no throughput E2E;
- redução aproximada de 44% na latência p95.

Fala sugerida:

> Isto é escala horizontal: aumentamos o número de instâncias, não a capacidade
> de uma única instância. Os Queue Groups fazem as réplicas da mesma etapa
> competirem pelas mensagens. Cada evento vai para somente uma réplica do grupo.

Não prometa crescimento linear. As réplicas compartilham a mesma VM de 1 OCPU e
os resultados variam com rede, CPU e carga anterior.

## 8. Queda de um contêiner durante a rajada

Mantenha duas réplicas. Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec dashboard \
  python benchmark.py --msgs 2000 --timeout 180 --label falha-payment
```

Durante o teste, na VM 2:

```bash
docker ps --filter label=com.docker.compose.project=tpsd-workers \
  --filter label=com.docker.compose.service=payment
```

Copie o ID de uma réplica e derrube-a:

```bash
docker kill --signal KILL ID_DA_REPLICA
```

Observe:

- a outra réplica continua recebendo novos eventos;
- `restart: unless-stopped` tenta reiniciar a réplica;
- mensagens em processamento no instante da queda podem ser perdidas;
- o resultado varia porque falha e processamento acontecem simultaneamente.

Restaure duas réplicas:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d --scale payment=2
```

Fala sugerida:

> Existe disponibilidade parcial porque outra réplica continua ativa, mas NATS
> Core usa at-most-once. Se o consumidor cair depois de receber e antes de
> publicar o próximo evento, não há ACK nem redelivery. Disponibilidade e
> garantia de entrega são propriedades diferentes.

## 9. Limitações assumidas

- O servidor NATS único é um ponto central de falha.
- NATS Core não persiste nem recupera mensagens.
- Queue Groups distribuem carga, mas não armazenam eventos.
- Não há transação distribuída entre as três etapas.
- Não há deduplicação persistente nem exactly-once.
- As VMs gratuitas de 1 OCPU tornam o benchmark sensível à carga.

Essas limitações deixam explícito o modelo adotado. Persistência, idempotência,
observabilidade avançada e redundância do broker são possíveis evoluções, mas
não fazem parte desta versão em NATS Core.

## 10. Encerramento sugerido

> O projeto demonstra um middleware conectando serviços distribuídos por
> pub/sub, Queue Groups e request/reply. A coreografia reduz o acoplamento, as
> réplicas mostram escala horizontal e a falha evidencia tanto a continuidade
> do serviço quanto o limite da entrega at-most-once.

## Plano B local

Se a nuvem ou a rede falhar:

```bash
docker compose up --build -d
docker compose ps
docker compose exec dashboard \
  python benchmark.py --msgs 500 --timeout 180 --label plano-b-local
```

O plano local mantém subjects, Queue Groups e fluxo lógico; muda apenas a
distribuição física entre hosts.

## Encerrar depois da apresentação

Na VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml down
```

Na VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml down
```

Não use `down -v`: o projeto não precisa de volumes persistentes.
