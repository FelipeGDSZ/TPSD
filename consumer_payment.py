"""Consumer de pagamento e primeira etapa da coreografia de pedidos."""

import asyncio
import json
import os
import random
import sys

from nats.aio.client import Client as NATS

from nats_connection import close_nats, connect_nats, wait_until_closed

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SUBJECT = "order.payment.*"
QUEUE_GROUP = "orders.payment"
NEXT_SUBJECT = "order.stock.reserve"


async def processar_pagamento(msg, nc: NATS) -> None:
    try:
        pedido = json.loads(msg.data.decode("utf-8"))
    except Exception as error:
        print(f"  [PAGAMENTO] Payload inválido: {error}")
        return

    try:
        order_id = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        amount = pedido.get("amount", 0.0)

        await asyncio.sleep(random.uniform(0.005, 0.05))
        print(
            f"  [PAGAMENTO] Aprovado | {order_id} | "
            f"R$ {amount:.2f} | Cliente {customer_id}"
        )

        await nc.publish(NEXT_SUBJECT, msg.data)
        await nc.flush(timeout=2)
        print(f"  [PAGAMENTO] Evento publicado em '{NEXT_SUBJECT}' | {order_id}")
    except Exception as error:
        print(f"  [PAGAMENTO] Erro ao processar mensagem: {error}")


async def main() -> None:
    nc = NATS()
    closed_event = asyncio.Event()
    print(f"[PAGAMENTO] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "PAGAMENTO", closed_event)

        async def handler(msg) -> None:
            await processar_pagamento(msg, nc)

        await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=handler)
        print(
            f"[PAGAMENTO] Ouvindo '{SUBJECT}' no grupo '{QUEUE_GROUP}'. "
            "Pressione Ctrl+C para sair."
        )
        await wait_until_closed(nc, closed_event)
    finally:
        print("[PAGAMENTO] Encerrando conexão com o NATS...")
        await close_nats(nc, "PAGAMENTO")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[PAGAMENTO] Falha fatal: {error}")
        sys.exit(1)
