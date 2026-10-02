"""Dashboard de monitoramento para NATS Core ou JetStream."""

import asyncio
import json
import os
import threading
import time

import requests
from flask import Flask, Response, jsonify, render_template, request

import producer

app = Flask(__name__)

NATS_MONITOR_URL = os.getenv("NATS_MONITOR_URL", "http://localhost:8222").rstrip("/")
NATS_MONITOR_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv("NATS_MONITOR_URLS", NATS_MONITOR_URL).split(",")
    if url.strip()
]

ultimo_estado = {
    "in_msgs": None,
    "out_msgs": None,
    "timestamp": None,
}
estado_lock = threading.Lock()
producer_slots = threading.BoundedSemaphore(
    value=int(os.getenv("DASHBOARD_MAX_PRODUCERS", "2"))
)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz")
def healthz():
    """Confirma que o dashboard consegue consultar o servidor NATS."""
    errors = []
    for monitor_url in NATS_MONITOR_URLS:
        try:
            response = requests.get(f"{monitor_url}/healthz", timeout=2)
            response.raise_for_status()
            return jsonify({"status": "ok", "nats": monitor_url})
        except requests.RequestException as error:
            errors.append(str(error))
    return jsonify({"status": "error", "detail": "; ".join(errors)}), 503


def buscar_metricas() -> dict:
    try:
        varz_documents = []
        connections = []
        errors = []
        for monitor_url in NATS_MONITOR_URLS:
            try:
                res_varz = requests.get(f"{monitor_url}/varz", timeout=2)
                res_varz.raise_for_status()
                varz_documents.append(res_varz.json())

                res_connz = requests.get(
                    f"{monitor_url}/connz?subs=true",
                    timeout=2,
                )
                res_connz.raise_for_status()
                connections.extend(res_connz.json().get("connections", []))
            except requests.RequestException as error:
                errors.append(f"{monitor_url}: {error}")

        if not varz_documents:
            raise ConnectionError("; ".join(errors) or "nenhum monitor NATS respondeu")

        agora = time.time()
        in_msgs_atual = sum(item.get("in_msgs", 0) for item in varz_documents)
        out_msgs_atual = sum(item.get("out_msgs", 0) for item in varz_documents)

        with estado_lock:
            estado_anterior = ultimo_estado.copy()
            if (
                estado_anterior["timestamp"] is None
                or in_msgs_atual < estado_anterior["in_msgs"]
                or out_msgs_atual < estado_anterior["out_msgs"]
            ):
                publish_rate = 0.0
                deliver_rate = 0.0
            else:
                delta_tempo = agora - estado_anterior["timestamp"]
                publish_rate = (
                    (in_msgs_atual - estado_anterior["in_msgs"]) / delta_tempo
                    if delta_tempo > 0
                    else 0.0
                )
                deliver_rate = (
                    (out_msgs_atual - estado_anterior["out_msgs"]) / delta_tempo
                    if delta_tempo > 0
                    else 0.0
                )

            ultimo_estado.update(
                {
                    "in_msgs": in_msgs_atual,
                    "out_msgs": out_msgs_atual,
                    "timestamp": agora,
                }
            )

        consumers = {
            "orders.payment": 0,
            "orders.stock": 0,
            "orders.notification": 0,
        }
        subject_to_queue = {
            "order.payment.*": "orders.payment",
            "order.stock.*": "orders.stock",
            "order.notify.*": "orders.notification",
        }
        connection_to_queue = {
            "pagamento": "orders.payment",
            "estoque": "orders.stock",
            "notificacao": "orders.notification",
        }

        for connection in connections:
            queue_by_name = connection_to_queue.get(connection.get("name"))
            if queue_by_name:
                consumers[queue_by_name] += 1
                continue
            subscriptions = connection.get("subscriptions_list", [])
            for subject, queue_name in subject_to_queue.items():
                if subject in subscriptions:
                    consumers[queue_name] += 1

        queues = {
            queue_name: {
                "messages": 0,
                "consumers": consumer_count,
                "publish_rate": publish_rate,
                "deliver_rate": deliver_rate,
            }
            for queue_name, consumer_count in consumers.items()
        }

        if os.getenv("NATS_MODE", "core").lower() == "jetstream":
            for monitor_url in NATS_MONITOR_URLS:
                try:
                    response = requests.get(
                        f"{monitor_url}/jsz?streams=true&consumers=true",
                        timeout=2,
                    )
                    response.raise_for_status()
                    apply_jetstream_metrics(queues, response.json())
                    break
                except requests.RequestException:
                    continue

        return {
            "status": "ok",
            "publish_rate": publish_rate,
            "deliver_rate": deliver_rate,
            "queues": queues,
        }
    except Exception as error:
        return {"error": str(error)}


def apply_jetstream_metrics(queues: dict, jsz: dict) -> None:
    """Adiciona backlog e redelivery dos durables do stream ORDERS."""
    durable_to_queue = {
        "orders-payment": "orders.payment",
        "orders-stock": "orders.stock",
        "orders-notification": "orders.notification",
    }
    for account in jsz.get("account_details", []):
        for stream in account.get("stream_detail", []):
            if stream.get("name") != os.getenv("JETSTREAM_STREAM", "ORDERS"):
                continue
            for consumer in stream.get("consumer_detail", []):
                queue_name = durable_to_queue.get(consumer.get("name"))
                if not queue_name:
                    continue
                pending = consumer.get("num_pending", 0)
                ack_pending = consumer.get("num_ack_pending", 0)
                queues[queue_name].update(
                    {
                        "messages": pending + ack_pending,
                        "pending": pending,
                        "ack_pending": ack_pending,
                        "redelivered": consumer.get("num_redelivered", 0),
                    }
                )


@app.route("/stream")
def stream():
    def gerador_eventos():
        while True:
            yield f"data: {json.dumps(buscar_metricas())}\n\n"
            time.sleep(1)

    return Response(gerador_eventos(), content_type="text/event-stream")


def executar_produtor_background(count: int) -> None:
    try:
        asyncio.run(producer.executar(total=count))
    except Exception as error:
        print(f"[DASHBOARD] Falha no produtor em background: {error}")
    finally:
        producer_slots.release()


@app.route("/api/produce", methods=["POST"])
def api_produce():
    try:
        data = request.get_json(silent=True) or {}
        count = int(data.get("count", 0))
        queue = data.get("queue", "orders.payment")

        if count != 1:
            return jsonify({"error": "Envie exatamente 1 mensagem por vez."}), 400
        if queue != "orders.payment":
            return (
                jsonify(
                    {
                        "error": (
                            "O fluxo E2E sempre começa em orders.payment; "
                            "estoque e notificação são acionados pela coreografia."
                        )
                    }
                ),
                400,
            )
        if not producer_slots.acquire(blocking=False):
            return jsonify({"error": "Já existem produtores em execução."}), 503

        try:
            thread = threading.Thread(
                target=executar_produtor_background,
                args=(count,),
                daemon=True,
            )
            thread.start()
        except Exception:
            producer_slots.release()
            raise

        return jsonify(
            {"status": "success", "message": "Fluxo de pedido iniciado."}
        )
    except (TypeError, ValueError) as error:
        return jsonify({"error": f"Requisição inválida: {error}"}), 400
    except Exception as error:
        print(f"[DASHBOARD] Erro em /api/produce: {error}")
        return jsonify({"error": "Não foi possível iniciar o produtor."}), 500


@app.route("/api/consume", methods=["POST"])
def api_consume():
    return jsonify(
        {
            "status": "info",
            "message": (
                "No NATS Core as mensagens não ficam retidas. "
                "Os consumers processam o fluxo em tempo real."
            ),
        }
    )


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    print("Dashboard iniciado em http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=debug)
