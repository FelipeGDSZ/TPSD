"""
dashboard.py (Versão NATS)
--------------------------
PAINEL DE MONITORAMENTO

Este script exibe o painel visual consultando a API nativa do NATS (porta 8222).
Ele envia os dados para o navegador usando SSE (Server-Sent Events).

Como executar:
  python dashboard.py

Depois, abra no navegador:
  http://localhost:5000
"""

from flask import Flask, Response, render_template, request, jsonify
import requests
import json
import time
import threading
import asyncio
import producer

app = Flask(__name__)

# URL da API de monitoramento nativa do NATS (não exige autenticação por padrão)
NATS_API_VARZ = "http://localhost:8222/varz"   # Métricas globais do servidor
NATS_API_CONNZ = "http://localhost:8222/connz" # Dados de clientes conectados

# Variáveis globais para calcular a taxa de mensagens por segundo
ultimo_estado = {
    "in_msgs": 0,
    "out_msgs": 0,
    "timestamp": time.time()
}

@app.route("/")
def index():
    """Serve a página principal do dashboard."""
    return render_template("index.html")

def buscar_metricas():
    """
    Consulta a API do NATS e retorna as taxas globais e clientes conectados.
    """
    global ultimo_estado
    
    try:
        # Pega as métricas globais do NATS
        res_varz = requests.get(NATS_API_VARZ, timeout=2)
        res_varz.raise_for_status()
        varz = res_varz.json()

        # Pega as conexões ativas para contar os consumidores
        res_connz = requests.get(NATS_API_CONNZ, timeout=2)
        res_connz.raise_for_status()
        connz = res_connz.json()

        # Cálculo de Taxa (Rate) manual, pois o NATS retorna totais acumulados
        agora = time.time()
        delta_tempo = agora - ultimo_estado["timestamp"]
        
        in_msgs_atual = varz.get("in_msgs", 0)
        out_msgs_atual = varz.get("out_msgs", 0)
        
        publish_rate = (in_msgs_atual - ultimo_estado["in_msgs"]) / delta_tempo if delta_tempo > 0 else 0
        deliver_rate = (out_msgs_atual - ultimo_estado["out_msgs"]) / delta_tempo if delta_tempo > 0 else 0
        
        # Atualiza o estado para o próximo ciclo
        ultimo_estado = {
            "in_msgs": in_msgs_atual,
            "out_msgs": out_msgs_atual,
            "timestamp": agora
        }

        # Contagem de consumidores agrupados pelas chaves que estamos usando
        # Para simplificar a visualização baseada no TP1 original
        consumidores_pagamento = 0
        consumidores_estoque = 0
        consumidores_notificacao = 0

        for conn in connz.get("connections", []):
            subs = conn.get("subscriptions_list", [])
            if "order.payment.*" in subs: consumidores_pagamento += 1
            if "order.stock.*" in subs: consumidores_estoque += 1
            if "order.notify.*" in subs: consumidores_notificacao += 1

        dados_filas = {
            "orders.payment": {
                "messages": 0, # NATS Core não retém mensagens, processa em tempo real
                "consumers": consumidores_pagamento,
                "publish_rate": publish_rate,
                "deliver_rate": deliver_rate
            },
            "orders.stock": {
                "messages": 0,
                "consumers": consumidores_estoque,
                "publish_rate": publish_rate,
                "deliver_rate": deliver_rate
            },
            "orders.notification": {
                "messages": 0,
                "consumers": consumidores_notificacao,
                "publish_rate": publish_rate,
                "deliver_rate": deliver_rate
            }
        }

        return {
            "status": "ok",
            "publish_rate": publish_rate,
            "deliver_rate": deliver_rate,
            "queues": dados_filas
        }
    except Exception as e:
        return {"error": str(e)}

@app.route("/stream")
def stream():
    """Endpoint SSE para empurrar métricas ao navegador a cada 1 segundo."""
    def gerador_eventos():
        while True:
            metricas = buscar_metricas()
            yield f"data: {json.dumps(metricas)}\n\n"
            time.sleep(1)
    return Response(gerador_eventos(), content_type="text/event-stream")

def executar_produtor_background(count, routing_key):
    """Wrapper para rodar o produtor assíncrono em uma thread síncrona do Flask."""
    asyncio.run(producer.executar(total=count, target_routing_key=routing_key))

@app.route("/api/produce", methods=["POST"])
def api_produce():
    try:
        data = request.json
        count = int(data.get("count", 0))
        queue = data.get("queue", "orders.payment")
        
        if count != 1:
            return jsonify({"error": "Para a apresentação, envie apenas 1 mensagem por vez."}), 400
            
        # Mapeamento da fila abstrata do front para o subject do NATS
        routing_key = None
        if queue == "orders.payment":
            routing_key = "order.payment.new"
        elif queue == "orders.stock":
            routing_key = "order.stock.reserve"
        elif queue == "orders.notification":
            routing_key = "order.notify.confirm"
            
        # Roda o script producer (que agora é async) em uma thread background
        threading.Thread(target=executar_produtor_background, args=(count, routing_key)).start()
        
        return jsonify({"status": "success", "message": f"Produzindo {count} mensagem..."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/consume", methods=["POST"])
def api_consume():
    """
    No RabbitMQ, tínhamos um botão para consumir as mensagens represadas na fila.
    No NATS Core, não existe represamento. O consumo manual não se aplica da mesma forma.
    """
    return jsonify({
        "status": "info", 
        "message": "Em NATS Core as mensagens não ficam retidas nas filas. Suba os consumidores via terminal para ver o processamento real-time!"
    })

if __name__ == "__main__":
    print("Dashboard iniciado! Acesse: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)