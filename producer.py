"""
producer.py (Versão NATS - Revisada)
------------------------------------
O PRODUTOR cria e envia pedidos para o NATS.
"""

import asyncio
import uuid
import json
import random
import time
import argparse
from datetime import datetime, timezone
from nats.aio.client import Client as NATS

# ── Configurações de conexão ───────────────────────────────────────
NATS_URL = "nats://localhost:4222"

SUBJECTS = [
    "orders.payment",
    "orders.stock",
    "orders.notification",
]

PRODUCTS = ["notebook", "smartphone", "tablet", "monitor", "headset", "keyboard", "mouse"]


def gerar_pedido(numero: int) -> dict:
    """Gera um pedido fictício com dados aleatórios."""
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{numero:06d}",
        "customer_id": f"CUST-{random.randint(1, 500):04d}",
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(19.99, 4999.99), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def executar(total: int, intervalo_log: int = 1000, target_subject: str = None):
    """Conecta ao NATS e envia `total` pedidos de forma assíncrona."""
    nc = NATS()
    print(f"[PRODUTOR] Conectando ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print(f"[PRODUTOR] Enviando {total:,} pedidos...\n")

    inicio = time.time()
    erros = 0
    
    # Atraso dinâmico para demorar até 10 segundos, para o usuário "ver" as mensagens no dashboard
    delay = min(0.1, 10.0 / total) if total > 0 else 0.0

    for i in range(1, total + 1):
        subject = target_subject if target_subject else random.choice(SUBJECTS)
        pedido = gerar_pedido(i)
        corpo = json.dumps(pedido).encode("utf-8")

        try:
            # Publica a mensagem no subject especificado
            await nc.publish(subject, corpo)
            
            # Pausa assíncrona (não bloqueia a thread) para a animação
            if delay > 0:
                await asyncio.sleep(delay)
                
        except Exception as e:
            erros += 1
            print(f"  [ERRO] Pedido {i}: {e}")
            continue

        # Exibe progresso a cada `intervalo_log` mensagens
        if i % intervalo_log == 0:
            decorrido = time.time() - inicio
            taxa = i / decorrido if decorrido > 0 else 0
            print(f"  Enviados: {i:>7,} | Tempo: {decorrido:>6.1f}s | Taxa: {taxa:>8.0f} msg/s")

    decorrido = time.time() - inicio
    taxa = total / decorrido if decorrido > 0 else 0

    print(f"\n{'='*50}")
    print(f"  TOTAL ENVIADO : {total:,} pedidos")
    print(f"  ERROS         : {erros}")
    print(f"  TEMPO TOTAL   : {decorrido:.2f}s")
    print(f"  TAXA MÉDIA    : {taxa:.0f} msg/s")
    print(f"{'='*50}")

    # Aguarda o envio de todas as mensagens que possam estar no buffer da rede
    await nc.drain()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produtor de pedidos – TP01 (Versão NATS)")
    parser.add_argument("--total", type=int, default=10000,
                        help="Quantos pedidos enviar (padrão: 10000)")
    parser.add_argument("--report", type=int, default=1000,
                        help="A cada quantos pedidos imprimir progresso (padrão: 1000)")
    parser.add_argument("--target", type=str, default=None,
                        help="Subject alvo específico (ex: orders.payment)")
    args = parser.parse_args()

    # Inicia o loop de eventos do asyncio para rodar a função
    asyncio.run(executar(total=args.total, intervalo_log=args.report, target_subject=args.target))