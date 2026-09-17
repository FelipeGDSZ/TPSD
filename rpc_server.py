"""
rpc_server.py
------------------------
Atua como um microsserviço de Estoque respondendo a consultas em tempo real.
"""

import asyncio
import json
from nats.aio.client import Client as NATS

# ── Configurações ──────────────────────────────────────────────────
NATS_URL = "nats://localhost:4222"

# Banco de dados fictício para a apresentação
ESTOQUE_DB = {
    "notebook": 50,
    "smartphone": 10,
    "tablet": 0,    # Produto esgotado!
    "monitor": 5
}

async def responder_consulta(msg):
    """
    Callback que recebe a pergunta, processa a lógica de negócio 
    e devolve a resposta diretamente ao solicitante.
    """
    try:
        dados = json.loads(msg.data.decode("utf-8"))
        produto = dados.get("product_id")
        
        # Validação básica: se não mandou o produto, levanta um erro
        if not produto:
            raise ValueError("O campo 'product_id' é obrigatório.")

        print(f"\n[RPC SERVER] Consulta recebida para o produto: '{produto}'")
        
        # Simula o tempo de latência de uma busca em um banco de dados real (50ms)
        await asyncio.sleep(0.05)
        
        # Regra de negócio: verifica no nosso "banco de dados"
        quantidade = ESTOQUE_DB.get(produto, 0)
        tem_estoque = quantidade > 0
        
        # Monta o payload de resposta com o status de sucesso
        resposta = {
            "product_id": produto,
            "available": tem_estoque,
            "stock": quantidade,
            "status": "success"
        }
        
        print(f"  -> Devolvendo resposta: {resposta}")
        
        # msg.respond() sabe exatamente para quem devolver 
        # a resposta através do "reply subject" embutido na mensagem original.
        await msg.respond(json.dumps(resposta).encode("utf-8"))

    except Exception as e:
        print(f"\n[RPC SERVER] Erro ao processar requisição: {e}")
        
        # Em um sistema resiliente, devolvemos uma mensagem de erro para não travar o cliente
        erro_resposta = {"error": str(e), "status": "failed"}
        
        # Verifica se a mensagem original exigia resposta antes de tentar devolver
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
    print("[RPC SERVER] Aguardando consultas no subject 'inventory.check'...")
    
    # O queue group "estoque_api" permite balanceamento de carga.
    # Se 10 clientes perguntarem ao mesmo tempo e houver 5 servidores rodando, 
    # o NATS distribui as perguntas igualmente entre eles de forma transparente.
    await nc.subscribe("inventory.check", queue="estoque_api", cb=responder_consulta)
    
    try:
        # Mantém o microsserviço ativo escutando as mensagens
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
        # Captura o Ctrl+C para encerrar sem cuspir erros feios no terminal
        pass