"""
consumer_payment.py (Versão NATS - Revisada)
--------------------------------------------
CONSUMIDOR DE PAGAMENTO

Este script fica "escutando" o subject 'order.payment.*' esperando pedidos chegarem.
No NATS, usamos 'Queue Groups' para simular o comportamento de uma fila do RabbitMQ.
Isso garante que se você rodar 5 instâncias deste script, a mensagem será 
entregue para apenas UM deles por vez (balanceamento de carga/round-robin).

Como funciona:
  1. Conecta ao NATS
  2. Assina o subject com o queue group 'orders.payment'
  3. Para cada pedido recebido, simula a validação e cobrança de forma assíncrona

Execute com:
  python consumer_payment.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
SUBJECT = "orders.payment"
QUEUE_GROUP = "payment_workers"


async def processar_pagamento(msg):
    """
    Callback assíncrona chamada automaticamente pelo NATS a cada mensagem.
    Substitui a lógica de ch.basic_consume do RabbitMQ.
    """
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        amount      = pedido.get("amount", 0.0)

        # Simula o tempo que um sistema real de pagamento levaria (5–50ms)
        # ATENÇÃO: É vital usar asyncio.sleep e não time.sleep para não travar
        # a thread principal do event loop quando várias mensagens chegarem juntas.
        await asyncio.sleep(random.uniform(0.005, 0.05))

        print(f"  [PAGAMENTO]  Aprovado | {order_id} | R$ {amount:.2f} | Cliente {customer_id}")
        
        # Nota sobre ACKs:
        # O NATS Core (padrão) é "fire-and-forget", ou seja, se a conexão cair, 
        # a mensagem é perdida e não há confirmação manual (msg.ack()).
        # Se usarmos o NATS JetStream (para ter persistência e DLQ como no RabbitMQ),
        # adicionaríamos 'await msg.ack()' aqui.

    except Exception as e:
        print(f"  [PAGAMENTO]  Erro ao processar mensagem: {e}")


async def main():
    nc = NATS()
    print(f"[PAGAMENTO] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print(f"[PAGAMENTO] Ouvindo o subject '{SUBJECT}' no grupo '{QUEUE_GROUP}'...")
    print(f"[PAGAMENTO] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    # A inscrição com "queue" faz o NATS balancear a carga entre os scripts idênticos.
    # Isso substitui totalmente a criação da fila e os bindings do código antigo.
    await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=processar_pagamento)

    try:
        # O Event().wait() pausa a execução desta função (mantendo o script vivo) 
        # para que o asyncio continue rodando as callbacks em background.
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[PAGAMENTO] Encerrando conexão limpa com o NATS...")
        # O drain() garante que não vamos fechar a conexão no meio de um processamento
        await nc.drain()


if __name__ == "__main__":
    try:
        # Ponto de entrada do script assíncrono
        asyncio.run(main())
    except KeyboardInterrupt:
        # Captura o Ctrl+C silenciosamente para não cuspir erro no terminal
        pass