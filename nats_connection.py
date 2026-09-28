"""Utilitários compartilhados para conexões resilientes com o NATS Core."""

import asyncio
from typing import Optional

from nats.aio.client import Client as NATS


async def connect_nats(
    nc: NATS,
    url: str,
    service_name: str,
    closed_event: Optional[asyncio.Event] = None,
) -> None:
    """Conecta ao NATS com callbacks e política de reconexão explícitas."""

    async def error_cb(error: Exception) -> None:
        print(f"[{service_name}] Erro NATS: {error}")

    async def disconnected_cb() -> None:
        if nc.is_draining or nc.is_closed:
            print(f"[{service_name}] Desconectando do NATS...")
        else:
            print(f"[{service_name}] Desconectado do NATS; aguardando reconexão...")

    async def reconnected_cb() -> None:
        connected_url = nc.connected_url
        url = connected_url.geturl() if connected_url is not None else "servidor desconhecido"
        print(f"[{service_name}] Reconectado ao NATS em {url}")

    async def closed_cb() -> None:
        print(f"[{service_name}] Conexão com o NATS encerrada.")
        if closed_event is not None:
            closed_event.set()

    await nc.connect(
        servers=[url],
        max_reconnect_attempts=60,
        reconnect_time_wait=2,
        error_cb=error_cb,
        disconnected_cb=disconnected_cb,
        reconnected_cb=reconnected_cb,
        closed_cb=closed_cb,
    )


async def wait_until_closed(nc: NATS, closed_event: asyncio.Event) -> None:
    """Mantém um serviço ativo e detecta fechamento definitivo da conexão."""
    while not closed_event.is_set():
        if nc.is_closed:
            closed_event.set()
            break
        try:
            await asyncio.wait_for(closed_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

    raise ConnectionError("A conexão com o NATS foi encerrada definitivamente.")


async def close_nats(nc: NATS, service_name: str) -> None:
    """Finaliza a conexão sem abandonar mensagens no buffer local."""
    try:
        if nc.is_connected:
            await nc.drain()
        elif not nc.is_closed:
            await nc.close()
    except Exception as error:
        print(f"[{service_name}] Erro ao encerrar conexão NATS: {error}")
