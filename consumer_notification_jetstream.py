"""
consumer_notification_jetstream.py (Versão JetStream)
-----------------------------------------------------
CONSUMIDOR DE NOTIFICAÇÕES COM JETSTREAM

Garante que nenhuma notificação seja perdida.

Execute com:
  python consumer_notification_jetstream.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS
from nats.js.errors import NotFoundError

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
STREAM_NAME = "ORDERS"
SUBJECT = "orders.notification"
DURABLE_NAME = "notification_durable"
CANAIS = ["email", "sms", "push"]


async def processar_notificacao(msg):
    """Callback com ACK manual para envio de notificações."""
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")

        await asyncio.sleep(random.uniform(0.002, 0.02))

        # Simula possível falha (2% de chance)
        if random.random() < 0.02:
            raise Exception("Falha no serviço de notificação")

        canal = random.choice(CANAIS)
        print(f"  [NOTIFICAÇÃO-JS] ✓ {canal.upper()} enviado | {order_id} | Cliente {customer_id} | seq={msg.metadata.sequence.stream}")
        await msg.ack()

    except Exception as e:
        print(f"  [NOTIFICAÇÃO-JS] ✗ Erro: {e} | Mensagem será reenviada")
        await msg.nak()


async def main():
    nc = NATS()
    print(f"[NOTIFICAÇÃO-JS] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    js = nc.jetstream()
    print(f"[NOTIFICAÇÃO-JS] JetStream habilitado ✓")

    # Verificar/criar stream
    try:
        await js.stream_info(STREAM_NAME)
        print(f"[NOTIFICAÇÃO-JS] Stream '{STREAM_NAME}' já existe")
    except NotFoundError:
        print(f"[NOTIFICAÇÃO-JS] Criando stream '{STREAM_NAME}'...")
        await js.add_stream(
            name=STREAM_NAME,
            subjects=["orders.*"],
            retention="limits",
            max_age=3600,
            storage="file"
        )
        print(f"[NOTIFICAÇÃO-JS] Stream '{STREAM_NAME}' criado ✓")

    print(f"[NOTIFICAÇÃO-JS] Ouvindo o subject '{SUBJECT}' (consumer durável: {DURABLE_NAME})...")
    print(f"[NOTIFICAÇÃO-JS] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    await js.subscribe(
        SUBJECT,
        durable=DURABLE_NAME,
        cb=processar_notificacao,
        manual_ack=True
    )

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[NOTIFICAÇÃO-JS] Encerrando conexão limpa com o NATS...")
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
