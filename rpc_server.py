"""Serviço RPC de consulta de estoque sobre NATS Core."""

import asyncio
import json
import os
import sys

from nats.aio.client import Client as NATS

from nats_connection import close_nats, connect_nats, wait_until_closed

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
RPC_SUBJECT = "inventory.check"
RPC_QUEUE_GROUP = "estoque_api"

ESTOQUE_DB = {
    "notebook": 50,
    "smartphone": 10,
    "tablet": 0,
    "monitor": 5,
}


async def responder_erro(msg, error: Exception) -> None:
    if not msg.reply:
        return
    try:
        resposta = {"error": str(error), "status": "failed"}
        await msg.respond(json.dumps(resposta).encode("utf-8"))
    except Exception as response_error:
        print(f"[RPC SERVER] Falha ao enviar resposta de erro: {response_error}")


async def responder_consulta(msg) -> None:
    try:
        dados = json.loads(msg.data.decode("utf-8"))
    except Exception as error:
        print(f"[RPC SERVER] Payload inválido: {error}")
        await responder_erro(msg, ValueError("payload inválido"))
        return

    try:
        produto = dados.get("product_id")
        if not produto:
            raise ValueError("O campo 'product_id' é obrigatório.")

        print(f"[RPC SERVER] Consulta recebida para o produto: '{produto}'")
        await asyncio.sleep(0.05)

        quantidade = ESTOQUE_DB.get(produto, 0)
        resposta = {
            "product_id": produto,
            "available": quantidade > 0,
            "stock": quantidade,
            "status": "success",
        }
        await msg.respond(json.dumps(resposta).encode("utf-8"))
    except Exception as error:
        print(f"[RPC SERVER] Erro ao processar requisição: {error}")
        await responder_erro(msg, error)


async def main() -> None:
    nc = NATS()
    closed_event = asyncio.Event()
    print(f"[RPC SERVER] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "RPC SERVER", closed_event)
        await nc.subscribe(
            RPC_SUBJECT,
            queue=RPC_QUEUE_GROUP,
            cb=responder_consulta,
        )
        print(
            f"[RPC SERVER] Aguardando consultas em '{RPC_SUBJECT}' "
            f"no grupo '{RPC_QUEUE_GROUP}'."
        )
        await wait_until_closed(nc, closed_event)
    finally:
        print("[RPC SERVER] Encerrando conexão com o NATS...")
        await close_nats(nc, "RPC SERVER")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[RPC SERVER] Falha fatal: {error}")
        sys.exit(1)
