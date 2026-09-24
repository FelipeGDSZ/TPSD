"""
consumer_notification.py (Versão NATS)
--------------------------------------
CONSUMIDOR DE NOTIFICAÇÕES

Este script fica "escutando" o subject 'order.notify.*' esperando pedidos.
Quando um pedido chega, ele simula o envio de uma notificação ao cliente
(e-mail, SMS ou push notification).

Como funciona:
  1. Conecta ao NATS
  2. Assina o subject com o queue group 'orders.notification'
  3. Para cada pedido, escolhe um canal de contato e "envia" de forma assíncrona

Execute com:
  python consumer_notification.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
SUBJECT = "orders.notification"
QUEUE_GROUP = "notification_workers"

# Canais de comunicação disponíveis para notificar o cliente
CANAIS = ["email", "sms", "push"]


async def processar_notificacao(msg):
    """
    Callback assíncrona chamada automaticamente pelo NATS a cada mensagem.
    Simula o envio de notificação por e-mail, SMS ou push.
    """
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")

        # Simula a latência de envio de uma notificação (2–20ms)
        await asyncio.sleep(random.uniform(0.002, 0.02))

        canal = random.choice(CANAIS)
        print(f"  [NOTIFICAÇÃO] {canal.upper()} enviado | {order_id} | Cliente {customer_id}")

    except Exception as e:
        print(f"  [NOTIFICAÇÃO]  Erro ao processar mensagem: {e}")


async def main():
    nc = NATS()
    print(f"[NOTIFICAÇÃO] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print(f"[NOTIFICAÇÃO] Ouvindo o subject '{SUBJECT}' no grupo '{QUEUE_GROUP}'...")
    print(f"[NOTIFICAÇÃO] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    # Inscrição usando Queue Group para balanceamento de carga (round-robin)
    await nc.subscribe(SUBJECT, queue=QUEUE_GROUP, cb=processar_notificacao)

    try:
        # Mantém o script vivo escutando os eventos
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[NOTIFICAÇÃO] Encerrando conexão limpa com o NATS...")
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass