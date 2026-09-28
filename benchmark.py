"""Benchmark simples do fluxo de pagamento em NATS Core."""

import argparse
import json
import os
import subprocess
import sys
import time

import matplotlib.pyplot as plt
import requests

RESULTS_FILE = os.getenv("BENCHMARK_RESULTS_FILE", "benchmark_results.json")
NATS_MONITOR_URL = os.getenv("NATS_MONITOR_URL", "http://localhost:8222").rstrip("/")
NATS_API_VARZ = f"{NATS_MONITOR_URL}/varz"


def get_delivered_messages() -> int:
    try:
        response = requests.get(NATS_API_VARZ, timeout=2)
        response.raise_for_status()
        return response.json().get("out_msgs", 0)
    except Exception as error:
        print(f"[API_ERRO] {error}")
        return -1


def stop_processes(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()

    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run_benchmark(msgs: int, consumers_count: int) -> None:
    print(f"\n[BENCHMARK] Iniciando {consumers_count} consumidores...")
    processes: list[subprocess.Popen] = []

    try:
        for _ in range(consumers_count):
            process = subprocess.Popen(
                [sys.executable, "consumer_payment.py"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append(process)

        time.sleep(2)
        start_msgs_count = get_delivered_messages()
        if start_msgs_count < 0:
            raise RuntimeError("Não foi possível ler as métricas iniciais do NATS.")

        print(f"[BENCHMARK] Gerando {msgs} mensagens...")
        start_time = time.time()

        subprocess.run(
            [
                sys.executable,
                "producer.py",
                "--total",
                str(msgs),
                "--target",
                "order.payment.new",
            ],
            check=True,
        )

        while True:
            current_msgs_count = get_delivered_messages()
            if current_msgs_count < 0:
                raise RuntimeError("A API de monitoramento do NATS ficou indisponível.")

            if current_msgs_count - start_msgs_count >= msgs:
                break

            if time.time() - start_time > 1800:
                raise TimeoutError("Timeout aguardando a entrega das mensagens.")

            time.sleep(0.5)

        elapsed = time.time() - start_time
        rate = msgs / elapsed if elapsed > 0 else 0
        print(
            f"[RESULTADO] {consumers_count} consumidores receberam {msgs} "
            f"mensagens em {elapsed:.2f}s => {rate:.0f} msg/s"
        )
        save_result(consumers_count, rate)
        generate_plot()
    finally:
        stop_processes(processes)


def save_result(consumers: int, rate: float) -> None:
    results = {}
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, "r", encoding="utf-8") as file:
                results = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            print(f"[AVISO] Resultado anterior ignorado: {error}")

    results[str(consumers)] = rate
    with open(RESULTS_FILE, "w", encoding="utf-8") as file:
        json.dump(results, file)


def generate_plot() -> None:
    if not os.path.exists(RESULTS_FILE):
        print("[AVISO] Nenhum resultado encontrado para plotar.")
        return

    with open(RESULTS_FILE, "r", encoding="utf-8") as file:
        results = json.load(file)

    if not results:
        return

    x = sorted(int(key) for key in results)
    y = [results[str(key)] for key in x]

    plt.figure(figsize=(8, 5))
    plt.plot(x, y, marker="o", linestyle="-", color="r", linewidth=2)
    plt.title("Throughput do NATS por Número de Consumidores")
    plt.xlabel("Número de consumidores (orders.payment)")
    plt.ylabel("Mensagens / Segundo")
    plt.xticks(x)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig("benchmark_plot_nats.png")
    plt.close()
    print("[PLOT] Gráfico salvo como 'benchmark_plot_nats.png'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Benchmark do NATS")
    parser.add_argument("--msgs", type=int, help="Total de mensagens")
    parser.add_argument("--consumers", type=int, help="Número de consumidores")
    parser.add_argument("--plot-only", action="store_true")
    args = parser.parse_args()

    if args.plot_only:
        generate_plot()
    elif args.msgs and args.consumers:
        run_benchmark(args.msgs, args.consumers)
    else:
        print("Uso: python benchmark.py --msgs X --consumers Y")
