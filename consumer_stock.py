"""Consumer de estoque e segunda etapa da coreografia de pedidos."""

import asyncio
import json
import os
import random
import sys

from nats.aio.client import Client as NATS

from nats_connection import close_nats, connect_nats, wait_until_closed

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SUBJECT = "order.stock.*"
QUEUE_GROUP = "orders.stock"
NEXT_SUBJECT = "order.notify.confirm"


async def processar_estoque(msg, nc: NATS) -> None:
    try:
        pedido = json.loads(msg.data.decode("utf-8"))
    except Exception as error:
        print(f"  [ESTOQUE] Payload inválido: {error}")
        return

    try:
        order_id = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        product_id = pedido.get("product_id", "?")
        quantity = pedido.get("quantity", 1)

        await asyncio.sleep(random.uniform(0.01, 0.03))
        print(
            f"  [ESTOQUE] Reservado | {order_id} | {quantity}x {product_id} | "
            f"Cliente {customer_id}"
        )

        await nc.publish(NEXT_SUBJECT, msg.data)
        await nc.flush(timeout=2)
        print(f"  [ESTOQUE] Evento publicado em '{NEXT_SUBJECT}' | {order_id}")
    except Exception as error:
        print(f"  [ESTOQUE] Erro ao processar mensagem: {error}")


async def main() -> None:
    nc = NATS()
    closed_event = asyncio.Event()
    print(f"[ESTOQUE] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "ESTOQUE", closed_event)

        async def handler(msg) -> None:
            await processar_estoque(msg, nc)

        await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=handler)
        print(
            f"[ESTOQUE] Ouvindo '{SUBJECT}' no grupo '{QUEUE_GROUP}'. "
            "Pressione Ctrl+C para sair."
        )
        await wait_until_closed(nc, closed_event)
    finally:
        print("[ESTOQUE] Encerrando conexão com o NATS...")
        await close_nats(nc, "ESTOQUE")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[ESTOQUE] Falha fatal: {error}")
        sys.exit(1)
