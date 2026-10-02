"""Primitivas compartilhadas para processamento confiavel com JetStream."""

import asyncio
import base64
import json
import os
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

JETSTREAM_STREAM = os.getenv("JETSTREAM_STREAM", "ORDERS")
NATS_MODE = os.getenv("NATS_MODE", "core").strip().lower()
FETCH_BATCH = max(1, int(os.getenv("JETSTREAM_FETCH_BATCH", "10")))
FETCH_TIMEOUT = max(0.5, float(os.getenv("JETSTREAM_FETCH_TIMEOUT", "1")))
ACK_WAIT = max(1, int(os.getenv("JETSTREAM_ACK_WAIT", "30")))
MAX_DELIVER = max(1, int(os.getenv("JETSTREAM_MAX_DELIVER", "5")))
MAX_ACK_PENDING = max(1, int(os.getenv("JETSTREAM_MAX_ACK_PENDING", "1000")))

PayloadProcessor = Callable[[dict, bytes, object], Awaitable[None]]


class InvalidPayloadError(ValueError):
    """Indica uma mensagem que nao deve ser tentada novamente."""


def jetstream_enabled() -> bool:
    if NATS_MODE not in {"core", "jetstream"}:
        raise ValueError("NATS_MODE deve ser 'core' ou 'jetstream'")
    return NATS_MODE == "jetstream"


def decode_payload(data: bytes) -> dict:
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception as error:
        raise InvalidPayloadError(str(error)) from error
    if not isinstance(payload, dict):
        raise InvalidPayloadError("o JSON deve ser um objeto")
    return payload


def message_id(payload: dict, destination: str) -> str:
    event_id = payload.get("event_id") or payload.get("order_id")
    if not event_id:
        raise InvalidPayloadError("event_id ou order_id e obrigatorio")
    return f"{event_id}:{destination}"


async def publish_jetstream(js, subject: str, data: bytes, payload: dict) -> None:
    """Publica e aguarda o PubAck da replica JetStream lider."""
    await js.publish(
        subject,
        data,
        headers={"Nats-Msg-Id": message_id(payload, subject)},
        timeout=5,
    )


async def publish_dlq(js, service_name: str, msg, error: Exception) -> None:
    subject = f"order.dlq.{service_name.lower()}"
    envelope = {
        "failed_service": service_name.lower(),
        "original_subject": msg.subject,
        "error": str(error),
        "payload_base64": base64.b64encode(msg.data).decode("ascii"),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    await js.publish(subject, data, timeout=5)


async def run_pull_consumer(
    nc: NATS,
    closed_event: asyncio.Event,
    subject: str,
    durable: str,
    service_name: str,
    processor: PayloadProcessor,
) -> None:
    """Compartilha um durable pull consumer entre todas as replicas do servico."""
    js = nc.jetstream(timeout=5)
    subscription = await js.pull_subscribe(
        subject=subject,
        durable=durable,
        stream=JETSTREAM_STREAM,
        config=ConsumerConfig(
            durable_name=durable,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=ACK_WAIT,
            max_deliver=MAX_DELIVER,
            max_ack_pending=MAX_ACK_PENDING,
            filter_subject=subject,
        ),
    )

    print(
        f"[{service_name}] JetStream ativo | stream={JETSTREAM_STREAM} "
        f"durable={durable} filtro={subject}"
    )

    while not closed_event.is_set() and not nc.is_closed:
        try:
            messages = await subscription.fetch(
                batch=FETCH_BATCH,
                timeout=FETCH_TIMEOUT,
            )
        except NatsTimeoutError:
            continue

        for msg in messages:
            try:
                payload = decode_payload(msg.data)
                await processor(payload, msg.data, js)
                await msg.ack_sync(timeout=2)
            except InvalidPayloadError as error:
                print(f"  [{service_name}] Payload invalido: {error}")
                try:
                    await publish_dlq(js, service_name, msg, error)
                    await msg.term()
                except Exception as dlq_error:
                    print(f"  [{service_name}] Falha ao publicar na DLQ: {dlq_error}")
                    await msg.nak(delay=2)
            except Exception as error:
                print(f"  [{service_name}] Erro transitorio; mensagem sera repetida: {error}")
                await msg.nak(delay=2)

    raise ConnectionError("A conexao com o NATS foi encerrada definitivamente.")
