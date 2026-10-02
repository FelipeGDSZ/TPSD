"""Benchmark E2E do fluxo coreografado sobre NATS Core ou JetStream."""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from nats.aio.client import Client as NATS

from jetstream_runtime import jetstream_enabled, publish_jetstream
from nats_connection import close_nats, connect_nats
from producer import PAYMENT_SUBJECT, gerar_pedido

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
FINAL_SUBJECT = "order.notify.confirm"
RESULTS_FILE = Path(os.getenv("BENCHMARK_RESULTS_FILE", "benchmark_results.json"))


def percentile(values: list[float], percentage: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentage))
    return ordered[index]


async def run_benchmark(
    total: int,
    timeout: float,
    batch_size: int,
    label: str,
) -> dict:
    """Publica uma rajada e observa cada pedido chegando à última etapa."""
    if total <= 0:
        raise ValueError("--msgs deve ser maior que zero")
    if batch_size <= 0:
        raise ValueError("--batch-size deve ser maior que zero")

    nc = NATS()
    benchmark_id = uuid.uuid4().hex
    completed = asyncio.Event()
    received_ids: set[str] = set()
    sent_at: dict[str, float] = {}
    latencies: list[float] = []

    async def observe_final_stage(msg) -> None:
        try:
            payload = json.loads(msg.data.decode("utf-8"))
            if payload.get("benchmark_id") != benchmark_id:
                return

            order_id = payload["order_id"]
            if order_id in received_ids:
                return

            received_ids.add(order_id)
            started = sent_at.get(order_id)
            if started is not None:
                latencies.append(time.monotonic() - started)

            if len(received_ids) >= total:
                completed.set()
        except Exception as error:
            print(f"[BENCHMARK] Evento final inválido: {error}")

    print(f"[BENCHMARK] Conectando ao NATS em {NATS_URL}...")
    try:
        await connect_nats(nc, NATS_URL, "BENCHMARK")
        js = nc.jetstream(timeout=5) if jetstream_enabled() else None
        subscription = await nc.subscribe(
            FINAL_SUBJECT,
            cb=observe_final_stage,
            pending_msgs_limit=max(65_536, total * 2),
            pending_bytes_limit=256 * 1024 * 1024,
        )
        await nc.flush(timeout=5)

        print(
            f"[BENCHMARK] Publicando rajada de {total:,} pedidos; "
            f"observando '{FINAL_SUBJECT}'."
        )
        started_at = time.monotonic()

        for index in range(1, total + 1):
            order = gerar_pedido(index)
            order["benchmark_id"] = benchmark_id
            order["benchmark_sequence"] = index
            sent_at[order["order_id"]] = time.monotonic()
            data = json.dumps(order).encode("utf-8")
            if js is None:
                await nc.publish(PAYMENT_SUBJECT, data)
            else:
                await publish_jetstream(js, PAYMENT_SUBJECT, data, order)
            if js is None and index % batch_size == 0:
                await nc.flush(timeout=10)

        if js is None:
            await nc.flush(timeout=10)
        publish_elapsed = time.monotonic() - started_at

        try:
            await asyncio.wait_for(completed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            print(
                f"[BENCHMARK] Timeout de {timeout:.0f}s; "
                "encerrando com os eventos observados."
            )

        elapsed = time.monotonic() - started_at
        await subscription.unsubscribe()

        delivered = len(received_ids)
        lost = total - delivered
        result = {
            "label": label,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sent": total,
            "completed": delivered,
            "lost": lost,
            "completion_percent": round((delivered / total) * 100, 2),
            "publish_seconds": round(publish_elapsed, 4),
            "total_seconds": round(elapsed, 4),
            "publish_rate": round(total / publish_elapsed, 2),
            "e2e_rate": round(delivered / elapsed, 2),
            "latency_ms": {
                "p50": round(percentile(latencies, 0.50) * 1000, 2),
                "p95": round(percentile(latencies, 0.95) * 1000, 2),
                "p99": round(percentile(latencies, 0.99) * 1000, 2),
            },
        }
        save_result(label, result)
        print_result(result)
        return result
    finally:
        await close_nats(nc, "BENCHMARK")


def save_result(label: str, result: dict) -> None:
    results = {}
    if RESULTS_FILE.exists():
        try:
            results = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            print(f"[BENCHMARK] Resultado anterior ignorado: {error}")

    results[label] = result
    RESULTS_FILE.write_text(
        json.dumps(results, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def print_result(result: dict) -> None:
    latency = result["latency_ms"]
    print("\n" + "=" * 64)
    print(f"  CENÁRIO       : {result['label']}")
    print(f"  ENVIADOS      : {result['sent']:,}")
    print(f"  ETAPA FINAL   : {result['completed']:,}")
    print(f"  PERDIDOS      : {result['lost']:,}")
    print(f"  CONCLUSÃO     : {result['completion_percent']:.2f}%")
    print(f"  PUBLICAÇÃO    : {result['publish_rate']:,.0f} msg/s")
    print(f"  E2E           : {result['e2e_rate']:,.0f} pedidos/s")
    print(
        "  LATÊNCIA      : "
        f"p50={latency['p50']:.1f}ms | "
        f"p95={latency['p95']:.1f}ms | "
        f"p99={latency['p99']:.1f}ms"
    )
    print("=" * 64)


def generate_plot() -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print(
            "[BENCHMARK] Matplotlib não está na imagem de runtime. "
            "Execute 'pip install -r requirements.txt' no ambiente local."
        )
        return

    if not RESULTS_FILE.exists():
        print("[BENCHMARK] Nenhum resultado encontrado para plotar.")
        return

    results = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    scenarios = []
    rates = []
    for label, result in results.items():
        if isinstance(result, dict) and "e2e_rate" in result:
            scenarios.append(label)
            rates.append(result["e2e_rate"])

    if not scenarios:
        print("[BENCHMARK] Nenhum resultado E2E encontrado.")
        return

    plt.figure(figsize=(9, 5))
    plt.bar(scenarios, rates, color="#26d9b3")
    plt.title("Throughput E2E por cenário")
    plt.xlabel("Cenário")
    plt.ylabel("Pedidos concluídos por segundo")
    plt.grid(axis="y", linestyle="--", alpha=0.35)
    plt.tight_layout()
    plt.savefig("benchmark_plot_nats.png")
    plt.close()
    print("[BENCHMARK] Gráfico salvo em benchmark_plot_nats.png")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark E2E do NATS")
    parser.add_argument("--msgs", type=int, default=1_000)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--label", default="manual")
    parser.add_argument("--plot-only", action="store_true")
    parser.add_argument("--fail-on-loss", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.plot_only:
        generate_plot()
    else:
        try:
            benchmark_result = asyncio.run(
                run_benchmark(
                    total=args.msgs,
                    timeout=args.timeout,
                    batch_size=args.batch_size,
                    label=args.label,
                )
            )
            if args.fail_on_loss and benchmark_result["lost"] > 0:
                sys.exit(2)
        except KeyboardInterrupt:
            pass
        except Exception as error:
            print(f"[BENCHMARK] Falha fatal: {error}")
            sys.exit(1)
