"""Cliente RPC de consulta de estoque sobre NATS Core."""

import asyncio
import json
import os
import sys

from nats.aio.client import Client as NATS

from nats_connection import close_nats, connect_nats

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
RPC_SUBJECT = "inventory.check"
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "2.0"))


async def main() -> None:
    nc = NATS()
    print(f"[RPC CLIENT] Conectando ao NATS em {NATS_URL}...")

    try:
        await connect_nats(nc, NATS_URL, "RPC CLIENT")
        produtos_para_testar = ["notebook", "tablet", "mouse", ""]

        for produto in produtos_para_testar:
            if produto:
                print(f"[RPC CLIENT] Consultando o produto '{produto}'...")
            else:
                print("[RPC CLIENT] Enviando requisição sem product_id...")

            pedido = {"product_id": produto}
            try:
                resposta_bruta = await nc.request(
                    RPC_SUBJECT,
                    json.dumps(pedido).encode("utf-8"),
                    timeout=RPC_TIMEOUT,
                )
                resposta = json.loads(resposta_bruta.data.decode("utf-8"))

                if resposta.get("status") == "failed":
                    print(f"  ERRO DO SERVIDOR: {resposta.get('error')}")
                elif resposta.get("available"):
                    print(
                        f"  SUCESSO: {resposta['stock']} unidades "
                        f"de '{produto}' disponíveis."
                    )
                else:
                    print(f"  RECUSADO: '{produto}' está fora de estoque.")
            except asyncio.TimeoutError:
                print("  TIMEOUT: servidor indisponível ou lento.")
            except Exception as error:
                print(f"  ERRO DE COMUNICAÇÃO: {error}")

            print("-" * 60)
            await asyncio.sleep(2)
    finally:
        print("[RPC CLIENT] Encerrando conexão com o NATS...")
        await close_nats(nc, "RPC CLIENT")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"[RPC CLIENT] Falha fatal: {error}")
        sys.exit(1)
