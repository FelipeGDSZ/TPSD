"""
consumer_payment.py (Versão Simplificada)
--------------------------------------------
CONSUMIDOR DE PAGAMENTO
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
SUBJECT = "pagamento"
QUEUE_GROUP = "orders.payment"

async def processar_pagamento(msg):
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        amount      = pedido.get("amount", 0.0)

        await asyncio.sleep(random.uniform(0.005, 0.05))

        print(f"  [PAGAMENTO]  Aprovado | {order_id} | R$ {amount:.2f} | Cliente {customer_id}")
        
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

    await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=processar_pagamento)

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[PAGAMENTO] Encerrando conexão limpa com o NATS...")
        await nc.drain()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass