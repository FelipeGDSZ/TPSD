"""
consumer_stock.py
-------------------------------
CONSUMIDOR DE ESTOQUE
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
SUBJECT = "estoque"
QUEUE_GROUP = "orders.stock"


async def processar_estoque(msg):
    """
    Callback assíncrona chamada automaticamente pelo NATS a cada mensagem.
    """
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        product_id  = pedido.get("product_id", "?")
        quantity    = pedido.get("quantity", 1)

        # Simula o tempo de processamento de baixa no banco de dados (10-30ms)
        await asyncio.sleep(random.uniform(0.01, 0.03))

        print(f"  [ESTOQUE]  Reservado | {order_id} | {quantity}x {product_id} | Cliente {customer_id}")

    except Exception as e:
        print(f"  [ESTOQUE]  Erro ao processar mensagem: {e}")


async def main():
    nc = NATS()
    print(f"[ESTOQUE] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print(f"[ESTOQUE] Ouvindo o subject '{SUBJECT}' no grupo '{QUEUE_GROUP}'...")
    print(f"[ESTOQUE] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    # A inscrição com o parâmetro "queue" garante a distribuição igualitária (round-robin)
    # substituindo as filas tradicionais do RabbitMQ.
    await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=processar_estoque)

    try:
        # Pausa a execução da main, deixando as callbacks rodarem no background
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[ESTOQUE] Encerrando conexão limpa com o NATS...")
        await nc.drain()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass