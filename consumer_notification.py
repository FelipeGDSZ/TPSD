"""Consumer de notificação e etapa final da coreografia de pedidos."""

import asyncio
import json
import os
import random
import sys

from nats.aio.client import Client as NATS

from nats_connection import close_nats, connect_nats, wait_until_closed

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SUBJECT = "order.notify.*"
QUEUE_GROUP = "orders.notification"
CANAIS = ["email", "sms", "push"]


async def processar_notificacao(msg) -> None:
    try:
        pedido = json.loads(msg.data.decode("utf-8"))
    except Exception as error:
        print(f"  [NOTIFICAÇÃO] Payload inválido: {error}")
        return

    try:
        order_id = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")

        await asyncio.sleep(random.uniform(0.002, 0.02))
        canal = random.choice(CANAIS)
        print(
            f"  [NOTIFICAÇÃO] {canal.upper()} enviado | "
            f"{order_id} | Cliente {customer_id}"
        )
    except Exception as error:
        print(f"  [NOTIFICAÇÃO] Erro ao processar mensagem: {error}")


async def main() -> None:
    nc = NATS()
    closed_event = asyncio.Event()
    print(f"[NOTIFICAÇÃO] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "NOTIFICAÇÃO", closed_event)
        await nc.subscribe(
            SUBJECT,
            queue=QUEUE_GROUP,
            cb=processar_notificacao,
        )
        print(
            f"[NOTIFICAÇÃO] Ouvindo '{SUBJECT}' no grupo '{QUEUE_GROUP}'. "
            "Pressione Ctrl+C para sair."
        )
        await wait_until_closed(nc, closed_event)
    finally:
        print("[NOTIFICAÇÃO] Encerrando conexão com o NATS...")
        await close_nats(nc, "NOTIFICAÇÃO")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[NOTIFICAÇÃO] Falha fatal: {error}")
        sys.exit(1)
