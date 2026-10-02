"""Produtor de pedidos para o fluxo coreografado em NATS Core ou JetStream."""

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from nats.aio.client import Client as NATS

from jetstream_runtime import jetstream_enabled, publish_jetstream
from nats_connection import close_nats, connect_nats

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
PAYMENT_SUBJECT = "order.payment.new"

PRODUCTS = [
    "notebook",
    "smartphone",
    "tablet",
    "monitor",
    "headset",
    "keyboard",
    "mouse",
]


def gerar_pedido(numero: int) -> dict:
    """Gera um pedido fictício com dados aleatórios."""
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": f"CUST-{random.randint(1, 500):04d}",
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(19.99, 4999.99), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def executar(
    total: int,
    intervalo_log: int = 1000,
    target_routing_key: str | None = None,
) -> None:
    """Publica pedidos exclusivamente na primeira etapa da coreografia."""
    if total < 0:
        raise ValueError("O total de pedidos não pode ser negativo.")

    subject = target_routing_key or PAYMENT_SUBJECT
    if subject != PAYMENT_SUBJECT:
        raise ValueError(
            f"O fluxo E2E deve começar em '{PAYMENT_SUBJECT}', não em '{subject}'."
        )

    nc = NATS()
    print(f"[PRODUTOR] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "PRODUTOR")
        js = nc.jetstream(timeout=5) if jetstream_enabled() else None
        print(f"[PRODUTOR] Enviando {total:,} pedidos para '{PAYMENT_SUBJECT}'...\n")

        inicio = time.time()
        erros = 0
        delay = min(0.1, 10.0 / total) if total > 0 else 0.0

        for i in range(1, total + 1):
            pedido = gerar_pedido(i)
            corpo = json.dumps(pedido).encode("utf-8")

            try:
                if js is None:
                    await nc.publish(PAYMENT_SUBJECT, corpo)
                else:
                    await publish_jetstream(js, PAYMENT_SUBJECT, corpo, pedido)
                if delay > 0:
                    await asyncio.sleep(delay)
            except Exception as error:
                erros += 1
                print(f"  [ERRO] Pedido {i}: {error}")
                continue

            if intervalo_log > 0 and i % intervalo_log == 0:
                decorrido = time.time() - inicio
                taxa = i / decorrido if decorrido > 0 else 0
                print(
                    f"  Enviados: {i:>7,} | Tempo: {decorrido:>6.1f}s | "
                    f"Taxa: {taxa:>8.0f} msg/s"
                )

        if js is None:
            await nc.flush(timeout=5)
        decorrido = time.time() - inicio
        enviados = total - erros
        taxa = enviados / decorrido if decorrido > 0 else 0

        print(f"\n{'=' * 50}")
        print(f"  TOTAL ENVIADO : {enviados:,} pedidos")
        print(f"  ERROS         : {erros}")
        print(f"  TEMPO TOTAL   : {decorrido:.2f}s")
        print(f"  TAXA MÉDIA    : {taxa:.0f} msg/s")
        print(f"{'=' * 50}")
    finally:
        await close_nats(nc, "PRODUTOR")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produtor de pedidos - NATS")
    parser.add_argument("--total", type=int, default=10000)
    parser.add_argument("--report", type=int, default=1000)
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help=f"Compatibilidade: aceita apenas {PAYMENT_SUBJECT}",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            executar(
                total=args.total,
                intervalo_log=args.report,
                target_routing_key=args.target,
            )
        )
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[PRODUTOR] Falha fatal: {error}")
        sys.exit(1)
