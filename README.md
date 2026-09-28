# TP1 Sistemas Distribuídos - NATS Core

Sistema de e-commerce distribuído em Python usando NATS Core, Queue Groups e
request/reply. A entrega é **at-most-once**: este projeto não usa JetStream.

## Fluxo dos pedidos

1. O producer publica em `order.payment.new`.
2. Pagamento publica o pedido aprovado em `order.stock.reserve`.
3. Estoque publica o pedido reservado em `order.notify.confirm`.
4. Notificação conclui o fluxo.

Cada etapa usa um Queue Group para permitir múltiplas instâncias do mesmo
consumer sem processamento simultâneo da mesma publicação.

## Execução local com Docker Compose

Pré-requisitos: Docker e Docker Compose.

```bash
docker compose up --build -d
docker compose ps
```

O Compose inicia NATS, pagamento, estoque, notificação, RPC e dashboard.

- Dashboard: http://localhost:5000
- Monitoramento NATS: http://localhost:8222
- Clientes NATS: `nats://localhost:4222`

Para acompanhar o fluxo:

```bash
docker compose logs -f payment stock notification
```

O dashboard inicia pedidos pela interface. Também é possível executar o
producer manualmente dentro da imagem:

```bash
docker compose exec dashboard python producer.py --total 10 --report 1
```

Teste do RPC:

```bash
docker compose exec dashboard python rpc_client.py
```

Encerramento:

```bash
docker compose down
```

## Execução sem Docker

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Com um servidor NATS disponível, configure os endpoints quando necessário:

```bash
export NATS_URL=nats://localhost:4222
export NATS_MONITOR_URL=http://localhost:8222
```

Em terminais separados, execute `consumer_payment.py`,
`consumer_stock.py`, `consumer_notification.py`, `rpc_server.py` e
`dashboard.py`.
