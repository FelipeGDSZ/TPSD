"""Healthchecks leves para os containers da aplicacao."""

import os
import socket
import sys
from urllib.parse import urlparse

import requests


def check_nats() -> None:
    parsed = urlparse(os.getenv("NATS_URL", "nats://localhost:4222"))
    if not parsed.hostname or not parsed.port:
        raise ValueError("NATS_URL invalida")
    with socket.create_connection((parsed.hostname, parsed.port), timeout=2):
        return


def check_dashboard() -> None:
    url = os.getenv("DASHBOARD_HEALTH_URL", "http://localhost:5000/healthz")
    response = requests.get(url, timeout=2)
    response.raise_for_status()


if __name__ == "__main__":
    checks = {"nats": check_nats, "dashboard": check_dashboard}
    try:
        checks[sys.argv[1]]()
    except (IndexError, KeyError):
        print("Uso: python healthcheck.py [nats|dashboard]", file=sys.stderr)
        sys.exit(2)
    except Exception as error:
        print(f"Healthcheck falhou: {error}", file=sys.stderr)
        sys.exit(1)
