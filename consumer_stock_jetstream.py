"""
consumer_stock_jetstream.py (Versão JetStream)
----------------------------------------------
CONSUMIDOR DE ESTOQUE COM JETSTREAM

Garante que nenhuma reserva de estoque seja perdida.

Execute com:
  python consumer_stock_jetstream.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS
from nats.js.errors import NotFoundError

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
STREAM_NAME = "ORDERS"
SUBJECT = "orders.stock"
DURABLE_NAME = "stock_durable"


async def processar_estoque(msg):
    """Callback com ACK manual para processamento de estoque."""
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        product_id  = pedido.get("product_id", "?")
        quantity    = pedido.get("quantity", 1)

        await asyncio.sleep(random.uniform(0.01, 0.03))

        # Simula possível falha (3% de chance)
        if random.random() < 0.03:
            raise Exception("Falha na consulta ao banco de estoque")

        print(f"  [ESTOQUE-JS] ✓ Reservado | {order_id} | {quantity}x {product_id} | Cliente {customer_id} | seq={msg.metadata.sequence.stream}")
        await msg.ack()

    except Exception as e:
        print(f"  [ESTOQUE-JS] ✗ Erro: {e} | Mensagem será reenviada")
        await msg.nak()


async def main():
    nc = NATS()
    print(f"[ESTOQUE-JS] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    js = nc.jetstream()
    print(f"[ESTOQUE-JS] JetStream habilitado ✓")

    # Verificar/criar stream
    try:
        await js.stream_info(STREAM_NAME)
        print(f"[ESTOQUE-JS] Stream '{STREAM_NAME}' já existe")
    except NotFoundError:
        print(f"[ESTOQUE-JS] Criando stream '{STREAM_NAME}'...")
        await js.add_stream(
            name=STREAM_NAME,
            subjects=["orders.*"],
            retention="limits",
            max_age=3600,
            storage="file"
        )
        print(f"[ESTOQUE-JS] Stream '{STREAM_NAME}' criado ✓")

    print(f"[ESTOQUE-JS] Ouvindo o subject '{SUBJECT}' (consumer durável: {DURABLE_NAME})...")
    print(f"[ESTOQUE-JS] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    await js.subscribe(
        SUBJECT,
        durable=DURABLE_NAME,
        cb=processar_estoque,
        manual_ack=True
    )

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[ESTOQUE-JS] Encerrando conexão limpa com o NATS...")
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
