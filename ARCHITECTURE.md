# Arquitetura do Sistema - TP01 Sistemas Distribuídos

## Visão Geral

Este projeto implementa um sistema de processamento de pedidos distribuído usando **NATS** como sistema de mensageria. O sistema demonstra conceitos fundamentais de sistemas distribuídos como comunicação assíncrona, balanceamento de carga, tolerância a falhas e comunicação síncrona (RPC).

## Diagrama da Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    NATS CLUSTER (3 nós)                      │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐          │
│  │  nats1   │◄────►│  nats2   │◄────►│  nats3   │          │
│  │ :4222    │      │ :4223    │      │ :4224    │          │
│  │ :8222    │      │ :8223    │      │ :8224    │          │
│  └──────────┘      └──────────┘      └──────────┘          │
│       ▲                                                      │
│       │ Clustering via Routes                               │
└───────┼──────────────────────────────────────────────────────┘
        │
        │ NATS Protocol (4222)
        │
┌───────┴──────────────────────────────────────────────────────┐
│                    COMPONENTES PYTHON                         │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐          Subjects / Queue Groups            │
│  │  Producer   │─────┐                                       │
│  │ (producer.py│     │                                       │
│  └─────────────┘     │    orders.payment      ──► 🔵 Payment│
│                      ├─►  orders.stock        ──► 🔵 Stock  │
│                      └─►  orders.notification ──► 🔵 Notify │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Consumers (Queue Groups para Load Balance)          │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │  🔵 consumer_payment.py    (queue: payment_workers)  │   │
│  │  🔵 consumer_stock.py      (queue: stock_workers)    │   │
│  │  🔵 consumer_notification.py (queue: notification_workers)│
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  RPC Pattern (Request-Reply)                         │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │  📡 rpc_server.py  (inventory.check)                 │   │
│  │  📞 rpc_client.py  (consulta síncrona)               │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Dashboard Web (Flask)                               │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │  🌐 dashboard.py (:5000)                             │   │
│  │     └─► Consulta API HTTP do NATS (:8222)           │   │
│  │     └─► Server-Sent Events (SSE) para frontend      │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

## Componentes do Sistema

### 1. NATS Cluster (Infraestrutura)

O cluster NATS é composto por 3 nós rodando em containers Docker:

- **nats1** (porta 4222, 8222): Nó principal (seed)
- **nats2** (porta 4223, 8223): Nó secundário
- **nats3** (porta 4224, 8224): Nó terciário

**Características:**
- Alta disponibilidade: Se um nó cair, os outros continuam operando
- Clustering automático via rotas configuradas
- API HTTP de monitoramento em cada nó (porta 8222)

### 2. Producer (Produtor de Mensagens)

**Arquivo:** `producer.py`

**Responsabilidades:**
- Gera pedidos fictícios com dados aleatórios
- Publica mensagens nos subjects do NATS
- Distribui mensagens entre os 3 subjects de forma aleatória ou direcionada

**Subjects publicados:**
- `orders.payment` - Pedidos para processamento de pagamento
- `orders.stock` - Pedidos para reserva de estoque
- `orders.notification` - Pedidos para envio de notificações

**Uso:**
```bash
python producer.py --total 10000 --target orders.payment
```

### 3. Consumers (Consumidores)

Os consumidores processam as mensagens de forma assíncrona usando **Queue Groups** do NATS para balanceamento de carga.

#### Consumer Payment (`consumer_payment.py`)
- **Subject:** `orders.payment`
- **Queue Group:** `payment_workers`
- **Função:** Simula validação e cobrança de pagamentos
- **Latência:** 5-50ms por pedido

#### Consumer Stock (`consumer_stock.py`)
- **Subject:** `orders.stock`
- **Queue Group:** `stock_workers`
- **Função:** Simula reserva de estoque
- **Latência:** 10-30ms por pedido

#### Consumer Notification (`consumer_notification.py`)
- **Subject:** `orders.notification`
- **Queue Group:** `notification_workers`
- **Função:** Simula envio de notificações (email, SMS, push)
- **Latência:** 2-20ms por pedido

**Balanceamento de Carga:**
- Múltiplas instâncias do mesmo consumer podem rodar simultaneamente
- O NATS distribui as mensagens automaticamente entre os consumers do mesmo Queue Group (round-robin)
- Garante que cada mensagem seja processada apenas uma vez

### 4. RPC (Request-Reply)

#### RPC Server (`rpc_server.py`)
- **Subject:** `inventory.check`
- **Função:** Microsserviço de consulta de estoque
- **Pattern:** Request-Reply síncrono
- **Queue Group:** `estoque_api` (permite múltiplas instâncias)

#### RPC Client (`rpc_client.py`)
- **Função:** Cliente que faz consultas síncronas ao servidor
- **Timeout:** 5 segundos
- **Uso:** Demonstra comunicação síncrona sobre NATS

**Banco de dados fictício:**
```python
{
    "notebook": 50,
    "smartphone": 10,
    "tablet": 0,    # Esgotado
    "monitor": 5
}
```

### 5. Dashboard Web (`dashboard.py`)

**Tecnologias:**
- Backend: Flask (Python)
- Frontend: HTML5 + Tailwind CSS + Vis.js
- Comunicação: Server-Sent Events (SSE)

**Funcionalidades:**
- Visualização em tempo real das métricas do NATS
- Produção manual de mensagens via interface web
- Monitoramento de consumidores conectados
- Taxas de publicação e entrega de mensagens

**APIs:**
- `GET /` - Interface web principal
- `GET /stream` - SSE endpoint para métricas em tempo real
- `POST /api/produce` - Produz mensagens via API
- `POST /api/consume` - Endpoint informativo (NATS Core não retém mensagens)

## Conceitos de Sistemas Distribuídos Implementados

### 1. Comunicação Assíncrona (Pub/Sub)
- Producers publicam mensagens sem esperar resposta
- Consumers processam mensagens de forma independente
- Desacoplamento entre produtores e consumidores

### 2. Balanceamento de Carga (Queue Groups)
- Distribuição automática de mensagens entre múltiplos consumers
- Escalabilidade horizontal (adicionar mais workers)
- Pattern: Competing Consumers

### 3. Tolerância a Falhas
- Cluster com 3 nós para alta disponibilidade
- Falha de um nó não impacta o sistema
- Recuperação automática quando o nó volta

### 4. Comunicação Síncrona (RPC)
- Request-Reply para consultas que exigem resposta imediata
- Timeout configurável
- Pattern: Remote Procedure Call

### 5. Monitoramento e Observabilidade
- API HTTP nativa do NATS para métricas
- Dashboard web para visualização em tempo real
- Métricas: mensagens in/out, taxa de throughput, consumers ativos

## Padrões de Mensageria Utilizados

### Pub/Sub (Publish-Subscribe)
```python
# Publisher
await nc.publish("orders.payment", data)

# Subscriber
await nc.subscribe("orders.payment", cb=callback)
```

### Queue Groups (Load Balancing)
```python
# Múltiplos consumers no mesmo grupo
await nc.subscribe("orders.payment", queue="payment_workers", cb=callback)
```

### Request-Reply (RPC)
```python
# Client
response = await nc.request("inventory.check", request_data, timeout=5)

# Server
await nc.subscribe("inventory.check", cb=handle_request)
# Inside callback:
await msg.respond(response_data)
```

## Tecnologias Utilizadas

- **NATS Server**: Sistema de mensageria de alta performance
- **Python 3.12+**: Linguagem de programação
- **nats-py**: Cliente NATS assíncrono para Python
- **asyncio**: Biblioteca para programação assíncrona
- **Flask**: Framework web para o dashboard
- **Docker & Docker Compose**: Containerização e orquestração
- **Vis.js**: Visualização de grafos para o frontend
- **Tailwind CSS**: Framework CSS para estilização

## Diferenças entre NATS Core e JetStream

Este projeto usa **NATS Core**, que oferece:
- ✅ Baixa latência (sub-milissegundo)
- ✅ Alta performance (milhões de msg/s)
- ✅ Pub/Sub simples e eficiente
- ❌ Sem persistência de mensagens
- ❌ Sem garantias de entrega
- ❌ Sem ACKs manuais

Para produção com requisitos de durabilidade, considere migrar para **NATS JetStream**:
- ✅ Persistência de mensagens
- ✅ At-least-once delivery
- ✅ ACKs manuais
- ✅ Dead Letter Queues
- ✅ Message replay

## Considerações de Escalabilidade

### Escalar Horizontalmente
```bash
# Adicionar mais consumers (em terminais separados)
python consumer_payment.py  # Instância 1
python consumer_payment.py  # Instância 2
python consumer_payment.py  # Instância 3
```

### Escalar o Cluster NATS
- Adicionar mais nós ao cluster modificando o `docker-compose.yml`
- Configurar rotas para todos os nós
- Recomendado: número ímpar de nós (3, 5, 7)

## Executando o Sistema

### Passo 1: Subir o Cluster NATS
```bash
docker compose up -d
```

### Passo 2: Verificar os Nós
```bash
docker ps
```

### Passo 3: Instalar Dependências Python
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Passo 4: Iniciar o Dashboard
```bash
python dashboard.py
# Acesse: http://localhost:5000
```

### Passo 5: Iniciar os Consumers (terminais separados)
```bash
python consumer_payment.py
python consumer_stock.py
python consumer_notification.py
```

### Passo 6: Produzir Mensagens
```bash
python producer.py --total 1000 --target orders.payment
```

### Passo 7: Testar RPC (opcional)
```bash
# Terminal 1
python rpc_server.py

# Terminal 2
python rpc_client.py
```

## Referências

- [NATS Documentation](https://docs.nats.io/)
- [nats-py GitHub](https://github.com/nats-io/nats.py)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [Padrões de Mensageria](https://www.enterpriseintegrationpatterns.com/)
