# NATS Core vs JetStream - Guia Comparativo

## Visão Geral

O NATS oferece dois modelos de mensageria diferentes:
- **NATS Core**: Sistema de mensageria em memória, ultra-rápido, fire-and-forget
- **JetStream**: Camada de persistência sobre o NATS Core com garantias de entrega

## Comparação Detalhada

| Característica | NATS Core | JetStream |
|----------------|-----------|-----------|
| **Persistência** | ❌ Apenas em memória | ✅ Mensagens persistidas em disco |
| **Garantia de Entrega** | ❌ At-most-once (fire-and-forget) | ✅ At-least-once, exactly-once |
| **ACK Manual** | ❌ Não disponível | ✅ Ack/Nack manual com retry |
| **Latência** | ✅ Sub-milissegundo | ⚠️ Alguns milissegundos |
| **Throughput** | ✅ Milhões msg/s | ⚠️ Centenas de milhares msg/s |
| **Message Replay** | ❌ Não disponível | ✅ Replay de mensagens antigas |
| **Dead Letter Queue** | ❌ Não disponível | ✅ Suporte nativo |
| **Retenção de Mensagens** | ❌ Perdidas se ninguém escuta | ✅ Retidas por tempo/tamanho |
| **Consumer Groups** | ✅ Queue Groups | ✅ Consumer Groups + Durable |
| **Ideal para** | Telemetria, eventos efêmeros | Transações, auditoria, filas críticas |

## NATS Core - Fire and Forget

### Como Funciona
- Mensagens publicadas são entregues **imediatamente** aos subscribers ativos
- Se nenhum subscriber está escutando, a mensagem é **perdida**
- Conexões perdidas = mensagens perdidas
- Sem histórico: não é possível recuperar mensagens antigas

### Casos de Uso Ideais
- ✅ Telemetria e métricas em tempo real
- ✅ Logs e eventos não-críticos
- ✅ Notificações push
- ✅ Streams de dados de IoT
- ✅ Cache invalidation
- ✅ Qualquer cenário onde perder algumas mensagens é aceitável

### Exemplo de Código
```python
import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    # Publisher - não espera confirmação
    await nc.publish("events", b"data")
    
    # Subscriber com callback
    async def handler(msg):
        print(f"Recebido: {msg.data}")
    
    await nc.subscribe("events", cb=handler)
    await asyncio.Event().wait()

asyncio.run(main())
```

## JetStream - Garantias de Entrega

### Como Funciona
- Mensagens são **persistidas em disco** antes de serem entregues
- Subscribers podem fazer **ACK manual** das mensagens
- Mensagens não confirmadas são **reenviadas automaticamente**
- Histórico completo: é possível **replay** de mensagens antigas
- Suporte a **Dead Letter Queues** para mensagens que falharam múltiplas vezes

### Casos de Uso Ideais
- ✅ Transações financeiras
- ✅ Ordens de compra e pedidos
- ✅ Eventos de auditoria
- ✅ Processamento de pagamentos
- ✅ Integração entre sistemas críticos
- ✅ Qualquer cenário onde perder mensagens é inaceitável

### Exemplo de Código
```python
import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    # Habilitar JetStream
    js = nc.jetstream()
    
    # Criar stream (similar a uma fila durável)
    await js.add_stream(
        name="ORDERS",
        subjects=["orders.*"],
        retention="limits",  # Ou "workqueue" para DLQ
        max_age=86400  # 24 horas
    )
    
    # Publisher com confirmação
    ack = await js.publish("orders.new", b"order_data")
    print(f"Mensagem publicada: {ack.seq}")
    
    # Consumer durável com ACK manual
    async def handler(msg):
        print(f"Processando: {msg.data}")
        # Processar a mensagem...
        await msg.ack()  # Confirma sucesso
        # Ou: await msg.nak() para reprocessar
    
    await js.subscribe("orders.*", cb=handler, durable="order-processor")
    await asyncio.Event().wait()

asyncio.run(main())
```

## Quando Usar Cada Um?

### Use NATS Core quando:
1. **Performance máxima** é prioritária (sub-ms latency)
2. Perder algumas mensagens **não é crítico**
3. Processamento em **tempo real** de eventos efêmeros
4. Volume **muito alto** de mensagens (milhões/s)
5. Simplicidade de implementação

### Use JetStream quando:
1. **Garantias de entrega** são obrigatórias
2. Mensagens precisam ser **persistidas**
3. Necessidade de **auditoria** e replay
4. Processamento **transacional**
5. Dead Letter Queues são necessárias
6. Recuperação de falhas sem perda de dados

## Migração: NATS Core → JetStream

### Passo 1: Habilitar JetStream no Servidor
```yaml
# docker-compose.yml
services:
  nats1:
    image: nats:latest
    command: >
      --name nats1
      --jetstream
      --store_dir /data
      --max_mem_store 1GB
      --max_file_store 10GB
    volumes:
      - ./nats-data:/data
```

### Passo 2: Atualizar Producer
```python
# Antes (NATS Core)
await nc.publish("orders.payment", data)

# Depois (JetStream)
js = nc.jetstream()
ack = await js.publish("orders.payment", data)
print(f"Persistida: seq={ack.seq}")
```

### Passo 3: Atualizar Consumer
```python
# Antes (NATS Core)
await nc.subscribe("orders.payment", queue="workers", cb=handler)

# Depois (JetStream)
js = nc.jetstream()
await js.subscribe(
    "orders.payment",
    queue="workers",
    durable="payment-processor",  # Consumer durável
    cb=handler_com_ack
)

async def handler_com_ack(msg):
    try:
        # Processar mensagem
        await processar(msg.data)
        await msg.ack()  # Confirma sucesso
    except Exception as e:
        await msg.nak()  # Reenvia para reprocessar
```

## Trade-offs

### NATS Core
**Vantagens:**
- ⚡ Latência ultra-baixa (sub-milissegundo)
- 🚀 Throughput altíssimo (milhões msg/s)
- 🎯 Simples de implementar
- 💾 Baixo uso de disco (tudo em memória)

**Desvantagens:**
- ⚠️ Sem garantias de entrega
- ⚠️ Mensagens perdidas em falhas
- ⚠️ Sem histórico ou replay
- ⚠️ Não adequado para dados críticos

### JetStream
**Vantagens:**
- ✅ At-least-once delivery garantido
- ✅ Persistência em disco
- ✅ ACKs manuais com retry
- ✅ Message replay e auditoria
- ✅ Dead Letter Queues

**Desvantagens:**
- 🐌 Latência maior (alguns ms)
- 📉 Throughput menor que Core
- 💾 Requer armazenamento em disco
- 🔧 Mais complexo de configurar

## Projeto Atual (TP01)

### Por que usamos NATS Core?
1. **Demonstração acadêmica**: Foco nos conceitos, não em produção
2. **Simplicidade**: Menos configuração, mais fácil de entender
3. **Performance**: Demonstrar o throughput máximo do NATS
4. **Suficiente para o caso de uso**: Pedidos fictícios não precisam de persistência

### Quando migrar para JetStream?
- Se o projeto evoluir para um sistema real de e-commerce
- Se precisar de auditoria de todas as transações
- Se a perda de pedidos for inaceitável
- Se precisar reprocessar pedidos antigos

## Comparação de Performance

### Benchmark NATS Core
```
Throughput: 2-5 milhões msg/s
Latência média: 0.1-0.5 ms
Tamanho da mensagem: 1 KB
Hardware: Commodity server
```

### Benchmark JetStream
```
Throughput: 100-500 mil msg/s
Latência média: 2-5 ms
Tamanho da mensagem: 1 KB
Hardware: Commodity server
Persistência: Habilitada (SSD)
```

## Conclusão

- **NATS Core**: Escolha padrão para eventos efêmeros e alta performance
- **JetStream**: Escolha obrigatória para dados críticos e transacionais
- Ambos podem coexistir no mesmo cluster
- É possível usar Core para eventos e JetStream para transações críticas

## Referências

- [NATS Core Documentation](https://docs.nats.io/nats-concepts/core-nats)
- [JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- [NATS Architecture](https://docs.nats.io/nats-concepts/architecture)
- [Performance Comparison](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/performance)
