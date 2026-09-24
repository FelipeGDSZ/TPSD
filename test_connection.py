#!/usr/bin/env python3
"""Script simples para testar a conexão com NATS"""

import asyncio
import sys
from nats.aio.client import Client as NATS

async def test():
    nc = NATS()
    print("[TEST] Tentando conectar ao NATS em nats://localhost:4222...")
    
    try:
        await nc.connect("nats://localhost:4222")
        print("[TEST] ✅ Conectado com sucesso!")
        
        # Testar publicação
        print("[TEST] Publicando mensagem de teste...")
        await nc.publish("test.subject", b"Hello NATS!")
        print("[TEST] ✅ Mensagem publicada!")
        
        # Fechar conexão
        await nc.close()
        print("[TEST] ✅ Conexão fechada corretamente!")
        return True
        
    except Exception as e:
        print(f"[TEST] ❌ ERRO: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test())
    sys.exit(0 if result else 1)
