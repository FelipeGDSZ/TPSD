"""Consumer de pagamento e primeira etapa da coreografia de pedidos."""

import asyncio
import os
import random
import sys

from nats.aio.client import Client as NATS

from jetstream_runtime import (
    InvalidPayloadError,
    decode_payload,
    jetstream_enabled,
    publish_jetstream,
    run_pull_consumer,
)
from nats_connection import close_nats, connect_nats, wait_until_closed

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SUBJECT = "order.payment.*"
QUEUE_GROUP = "orders.payment"
DURABLE = "orders-payment"
NEXT_SUBJECT = "order.stock.reserve"
LOG_EVERY = max(1, int(os.getenv("PROCESS_LOG_EVERY", "1")))
processed_messages = 0


async def processar_pedido(pedido: dict, data: bytes, nc: NATS, js=None) -> None:
    global processed_messages
    processed_messages += 1
    log_this = processed_messages == 1 or processed_messages % LOG_EVERY == 0
    order_id = pedido.get("order_id", "?")
    customer_id = pedido.get("customer_id", "?")
    amount = pedido.get("amount", 0.0)

    await asyncio.sleep(random.uniform(0.005, 0.05))
    if log_this:
        print(
            f"  [PAGAMENTO] Aprovado | {order_id} | "
            f"R$ {amount:.2f} | Cliente {customer_id}"
        )

    if js is None:
        await nc.publish(NEXT_SUBJECT, data)
        await nc.flush(timeout=2)
    else:
        await publish_jetstream(js, NEXT_SUBJECT, data, pedido)
    if log_this:
        print(f"  [PAGAMENTO] Evento publicado em '{NEXT_SUBJECT}' | {order_id}")


async def processar_pagamento(msg, nc: NATS) -> None:
    try:
        pedido = decode_payload(msg.data)
        await processar_pedido(pedido, msg.data, nc)
    except InvalidPayloadError as error:
        print(f"  [PAGAMENTO] Payload inválido: {error}")
    except Exception as error:
        print(f"  [PAGAMENTO] Erro ao processar mensagem: {error}")


async def main() -> None:
    nc = NATS()
    closed_event = asyncio.Event()
    print(f"[PAGAMENTO] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "PAGAMENTO", closed_event)

        if jetstream_enabled():
            async def process_jetstream(pedido: dict, data: bytes, js) -> None:
                await processar_pedido(pedido, data, nc, js)

            await run_pull_consumer(
                nc, closed_event, SUBJECT, DURABLE, "PAGAMENTO", process_jetstream
            )
        else:
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
