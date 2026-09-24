"""
consumer_payment_jetstream.py (Versão JetStream)
------------------------------------------------
CONSUMIDOR DE PAGAMENTO COM JETSTREAM

Este script usa JetStream para garantias de entrega:
- Mensagens persistidas em disco
- ACK manual (confirmação de processamento)
- Reentrega automática em caso de falha
- Consumer durável (mantém posição mesmo após restart)

Execute com:
  python consumer_payment_jetstream.py
"""

import asyncio
import json
import random
from nats.aio.client import Client as NATS
from nats.js.errors import NotFoundError

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"
STREAM_NAME = "ORDERS"
SUBJECT = "orders.payment"
CONSUMER_NAME = "payment_processor"
DURABLE_NAME = "payment_durable"


async def processar_pagamento(msg):
    """
    Callback assíncrona com ACK manual.
    Se o processamento falhar, a mensagem será reenviada automaticamente.
    """
    try:
        pedido = json.loads(msg.data.decode("utf-8"))

        order_id    = pedido.get("order_id", "?")
        customer_id = pedido.get("customer_id", "?")
        amount      = pedido.get("amount", 0.0)

        # Simula o tempo de processamento
        await asyncio.sleep(random.uniform(0.005, 0.05))

        # Simula possível falha (5% de chance)
        if random.random() < 0.05:
            raise Exception("Falha simulada no processamento")

        print(f"  [PAGAMENTO-JS] ✓ Aprovado | {order_id} | R$ {amount:.2f} | Cliente {customer_id} | seq={msg.metadata.sequence.stream}")
        
        # ACK manual: confirma que a mensagem foi processada com sucesso
        await msg.ack()

    except Exception as e:
        print(f"  [PAGAMENTO-JS] ✗ Erro: {e} | Mensagem será reenviada")
        # NAK: informa que houve erro e a mensagem deve ser reenviada
        await msg.nak()


async def main():
    nc = NATS()
    print(f"[PAGAMENTO-JS] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    # Habilitar JetStream
    js = nc.jetstream()
    print(f"[PAGAMENTO-JS] JetStream habilitado ✓")

    # Verificar se o stream já existe, caso contrário criar
    try:
        stream_info = await js.stream_info(STREAM_NAME)
        print(f"[PAGAMENTO-JS] Stream '{STREAM_NAME}' já existe")
    except NotFoundError:
        print(f"[PAGAMENTO-JS] Criando stream '{STREAM_NAME}'...")
        await js.add_stream(
            name=STREAM_NAME,
            subjects=["orders.*"],
            retention="limits",  # Limites de tempo/tamanho
            max_age=3600,  # Mantém mensagens por 1 hora
            storage="file"  # Persistência em disco
        )
        print(f"[PAGAMENTO-JS] Stream '{STREAM_NAME}' criado ✓")

    print(f"[PAGAMENTO-JS] Ouvindo o subject '{SUBJECT}' (consumer durável: {DURABLE_NAME})...")
    print(f"[PAGAMENTO-JS] Aguardando pedidos. Pressione Ctrl+C para sair.\n")

    # Subscribe com consumer durável
    # O consumer durável mantém o estado mesmo se o script for reiniciado
    await js.subscribe(
        SUBJECT,
        durable=DURABLE_NAME,
        cb=processar_pagamento,
        manual_ack=True  # Requer ACK manual
    )

    try:
        # Mantém o script vivo escutando os eventos
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[PAGAMENTO-JS] Encerrando conexão limpa com o NATS...")
        await nc.drain()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
