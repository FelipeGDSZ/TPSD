"""Cria ou atualiza o stream compartilhado pelo fluxo de pedidos."""

import asyncio
import os
import sys

from nats.aio.client import Client as NATS
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from nats_connection import close_nats, connect_nats
from jetstream_runtime import JETSTREAM_STREAM

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")


async def main() -> None:
    nc = NATS()
    config = StreamConfig(
        name=JETSTREAM_STREAM,
        subjects=["order.>"],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.OLD,
        max_age=3600,
        max_bytes=1024 * 1024 * 1024,
        max_msgs=100_000,
        duplicate_window=600,
        num_replicas=3,
    )

    print(f"[JETSTREAM SETUP] Conectando ao NATS em {NATS_URL}...")
    try:
        await connect_nats(nc, NATS_URL, "JETSTREAM SETUP")
        js = nc.jetstream(timeout=10)
        try:
            await js.stream_info(JETSTREAM_STREAM)
        except NotFoundError:
            await js.add_stream(config=config)
            action = "criado"
        else:
            await js.update_stream(config=config)
            action = "atualizado"

        info = await js.stream_info(JETSTREAM_STREAM)
        print(
            f"[JETSTREAM SETUP] Stream {info.config.name} {action} | "
            f"replicas={info.config.num_replicas} subjects={info.config.subjects}"
        )
    finally:
        await close_nats(nc, "JETSTREAM SETUP")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        print(f"[JETSTREAM SETUP] Falha fatal: {error}")
        sys.exit(1)
