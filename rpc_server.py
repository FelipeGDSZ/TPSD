"""
rpc_server.py 
------------------------
Atua como um microsserviço de Estoque respondendo a consultas em tempo real.
"""

import asyncio
import json
from nats.aio.client import Client as NATS

NATS_URL = "nats://localhost:4222"

ESTOQUE_DB = {
    "notebook": 50,
    "smartphone": 10,
    "tablet": 0,
    "monitor": 5
}

async def responder_consulta(msg):
    try:
        dados = json.loads(msg.data.decode("utf-8"))
        produto = dados.get("product_id")
        
        if not produto:
            raise ValueError("O campo 'product_id' é obrigatório.")

        print(f"\n[RPC SERVER] Consulta recebida para o produto: '{produto}'")
        
        await asyncio.sleep(0.05)
        
        quantidade = ESTOQUE_DB.get(produto, 0)
        tem_estoque = quantidade > 0
        
        resposta = {
            "product_id": produto,
            "available": tem_estoque,
            "stock": quantidade,
            "status": "success"
        }
        
        print(f"  -> Devolvendo resposta: {resposta}")
        
        await msg.respond(json.dumps(resposta).encode("utf-8"))

    except Exception as e:
        print(f"\n[RPC SERVER] Erro ao processar requisição: {e}")
        
        erro_resposta = {"error": str(e), "status": "failed"}
        
        if msg.reply:
            await msg.respond(json.dumps(erro_resposta).encode("utf-8"))

async def main():
    nc = NATS()
    print(f"[RPC SERVER] Tentando conectar ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print("[RPC SERVER] Conectado!")
    print("[RPC SERVER] Aguardando consultas no subject 'consulta_estoque'...")
    
    # Atualizado para o novo subject em português
    await nc.subscribe("consulta_estoque", queue="estoque_api", cb=responder_consulta)
    
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[RPC SERVER] Encerrando conexão limpa com o NATS...")
        await nc.drain()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass