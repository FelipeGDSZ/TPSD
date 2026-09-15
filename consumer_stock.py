"""
consumer_stock.py (Versão NATS)
-------------------------------
CONSUMIDOR DE ESTOQUE

Este script fica "escutando" o subject 'order.stock.*' esperando pedidos chegarem.
Utiliza 'Queue Groups' do NATS para garantir que a reserva do estoque de uma
mesma mensagem não seja duplicada caso haja múltiplas instâncias deste script rodando.

Como funciona:
  1. Conecta ao NATS
  2. Assina o subject com o queue group 'orders.stock'
  3. Para cada pedido, verifica os produtos de forma assíncrona

Execute com:
  python consumer_stock.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
SUBJECT = "order.stock.*"
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

        # Simula o tempo de consulta ao sistema de estoque (10–30ms)
        # Fundamental usar asyncio.sleep para não bloquear o loop de eventos
        await asyncio.sleep(random.uniform(0.01, 0.03))

        print(f"  [ESTOQUE] ✅ Reservado | {order_id} | {quantity}x {product_id} | Cliente {customer_id}")

    except Exception as e:
        print(f"  [ESTOQUE] ❌ Erro ao processar mensagem: {e}")


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
    await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=processar_estoque)

    try:
        # Pausa a execução da main, deixando os callbacks rodarem no background
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