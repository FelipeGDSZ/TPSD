"""
benchmark.py (Versão NATS)
--------------------------
Script para testar o limite de vazão (Throughput) do NATS.
"""

import argparse
import subprocess
import time
import requests
import json
import os
import matplotlib.pyplot as plt

RESULTS_FILE = "benchmark_results.json"
# Usamos a API de variáveis de status do servidor NATS
NATS_API_VARZ = "http://localhost:8222/varz"

def get_delivered_messages():
    """
    Busca o total global de mensagens já entregues (out_msgs) pelo NATS aos consumidores.
    """
    try:
        resp = requests.get(NATS_API_VARZ, timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("out_msgs", 0)
    except Exception as e:
        print(f"[API_ERRO] {e}")
    return -1

def run_benchmark(msgs, consumers_count):
    # Não há "purge_queues()" no NATS Core porque as mensagens não ficam retidas.
    # Se ninguém estiver ouvindo, elas simplesmente não existem.
    
    print(f"\n[BENCHMARK] Iniciando {consumers_count} consumidores (workers)...")
    processes = []
    
    # Inicia os consumidores primeiro para garantir que não percam as mensagens 
    # (já que o NATS é fire-and-forget)
    for _ in range(consumers_count):
        p = subprocess.Popen(["python", "consumer_payment.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(p)
    
    # Dá um tempinho para os consumidores se conectarem ao NATS
    time.sleep(2)
    
    # Captura a quantidade de mensagens processadas antes do teste começar
    start_msgs_count = get_delivered_messages()
    
    print(f"\n[BENCHMARK] Gerando {msgs} mensagens...")
    start_time = time.time()
    
    # O produtor agora é chamado para publicar o mais rápido possível
    subprocess.run(["python", "producer.py", "--total", str(msgs), "--target", "order.payment.new"], check=True)
    
    # Aguarda até que o NATS confirme a entrega de todas as mensagens aos consumidores
    while True:
        current_msgs_count = get_delivered_messages()
        delta = current_msgs_count - start_msgs_count
        
        if delta >= msgs:
            break
            
        # Timeout preventivo (30 mins max) para não travar o PC
        if time.time() - start_time > 1800:
            print("[BENCHMARK] Timeout excedido! Algumas mensagens podem ter sido perdidas (Slow Consumer).")
            break
        time.sleep(0.5)
        
    elapsed = time.time() - start_time
    rate = msgs / elapsed if elapsed > 0 else 0
    
    print(f"\n[RESULTADO] {consumers_count} consumidores processaram {msgs} msgs em {elapsed:.2f}s => {rate:.0f} msg/s")
    
    # Finalizando processos dos consumidores
    for p in processes:
        p.terminate()
        
    save_result(consumers_count, rate)
    generate_plot()

def save_result(consumers, rate):
    results = {}
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r") as f:
            try:
                results = json.load(f)
            except:
                pass
    results[str(consumers)] = rate
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f)

def generate_plot():
    if not os.path.exists(RESULTS_FILE):
        print("[AVISO] Nenhum arquivo de resultados encontrado para plotar.")
        return
        
    with open(RESULTS_FILE, "r") as f:
        results = json.load(f)
        
    if not results: return
    
    x = sorted([int(k) for k in results.keys()])
    y = [results[str(k)] for k in x]
    
    plt.figure(figsize=(8, 5))
    plt.plot(x, y, marker='o', linestyle='-', color='r', linewidth=2) # Linha vermelha
    plt.title("Throughput do NATS por Número de Consumidores")
    plt.xlabel("Número de Consumidores (Queue Group: orders.payment)")
    plt.ylabel("Mensagens / Segundo (Vazão)")
    plt.xticks(x)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("benchmark_plot_nats.png")
    print("\n[PLOT] Gráfico gerado e salvo como 'benchmark_plot_nats.png'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Benchmark do NATS")
    parser.add_argument("--msgs", type=int, help="Total de msgs para simular")
    parser.add_argument("--consumers", type=int, help="Numero de consumidores")
    parser.add_argument("--plot-only", action="store_true", help="Gera apenas o grafico com base nos resultados ja gravados")
    args = parser.parse_args()
    
    if args.plot_only:
        generate_plot()
    elif args.msgs and args.consumers:
        run_benchmark(args.msgs, args.consumers)
    else:
        print("Uso: python benchmark.py --msgs X --consumers Y")