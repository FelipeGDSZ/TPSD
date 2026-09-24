"""
rpc_client.py
------------------------
Faz consultas diretas (Request-Reply) ao microsserviço de estoque 
e espera a resposta para tomar uma decisão.
"""

import asyncio
import json
from nats.aio.client import Client as NATS

NATS_URL = "nats://localhost:4222"

async def main():
    nc = NATS()
    print(f"[RPC CLIENT] Tentando conectar ao NATS em {NATS_URL}...")
    
    try:
        await nc.connect(NATS_URL)
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no servidor NATS: {e}")
        return

    print("[RPC CLIENT]  Conectado. Iniciando bateria de consultas...\n")
    
    produtos_para_testar = ["notebook", "tablet", "mouse", ""]
    
    for produto in produtos_para_testar:
        if produto == "":
            print("[RPC CLIENT] Teste de Resiliência: Enviando requisição sem product_id...")
        else:
            print(f"[RPC CLIENT] Perguntando ao estoque se tem '{produto}'...")
            
        pedido = {"product_id": produto}
        
        try:
            # Atualizado para disparar a requisição no subject em português
            resposta_bruta = await nc.request("consulta_estoque", json.dumps(pedido).encode("utf-8"), timeout=2.0)
            
            resposta = json.loads(resposta_bruta.data.decode("utf-8"))
            
            if resposta.get("status") == "failed":
                print(f"  ERRO RETORNADO PELO SERVIDOR: {resposta.get('error')}")
            else:
                if resposta.get("available"):
                    print(f"   SUCESSO: Temos {resposta['stock']} unidades de '{produto}'. Pode vender!")
                else:
                    print(f"   RECUSADO: O produto '{produto}' está fora de estoque.")
                
        except asyncio.TimeoutError:
            print("   TIMEOUT: O servidor de estoque demorou demais ou está offline.")
        except Exception as e:
            print(f"   ERRO DE COMUNICAÇÃO: {e}")
        
        print("-" * 60)
        await asyncio.sleep(2)

    print("\n[RPC CLIENT] Testes finalizados. Encerrando conexão...")
    await nc.drain()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass