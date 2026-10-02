# TP1 Sistemas Distribuídos — NATS Core

Guia de execução e testes do projeto.

Para a evolução local com Kubernetes, cluster NATS e JetStream, consulte
[`KUBERNETES.md`](KUBERNETES.md).

## Pré-requisitos

```bash
docker --version
docker compose version
git --version
```

## Clonar o projeto

```bash
git clone -b fix/nats-core-local-stability \
  https://github.com/FelipeGDSZ/TPSD.git
cd TPSD
```

## Validação estática

```bash
python3 -m py_compile *.py
docker compose config --quiet
```

## Execução local com Docker Compose

```bash
docker compose up --build -d
docker compose ps
```

Aguarde todos os serviços ficarem `healthy`.

```bash
curl --fail --silent --show-error http://127.0.0.1:5000/healthz
curl --fail --silent --show-error http://127.0.0.1:8222/healthz
```

Endereços locais:

```text
Dashboard:          http://localhost:5000
Monitoramento NATS: http://localhost:8222
NATS:               nats://localhost:4222
```

## Teste ponta a ponta

Terminal 1:

```bash
docker compose logs -f --tail=0 payment stock notification
```

Terminal 2:

```bash
docker compose exec -T dashboard \
  python producer.py --total 10 --report 1
```

Interrompa somente a exibição dos logs com `Ctrl+C`.

## Teste pelo dashboard

```bash
xdg-open http://localhost:5000
```

Alternativa:

```text
Abra http://localhost:5000 no navegador.
```

## Teste RPC

```bash
docker compose exec -T dashboard python rpc_client.py
```

## Monitoramento do NATS

```bash
curl --fail --silent --show-error http://127.0.0.1:8222/varz
curl --fail --silent --show-error \
  'http://127.0.0.1:8222/connz?subs=true'
```

## Benchmark com um worker por etapa

```bash
docker compose up -d \
  --scale payment=1 \
  --scale stock=1 \
  --scale notification=1
docker compose ps
```

Aguarde todos ficarem `healthy`.

```bash
docker compose exec -T dashboard \
  python benchmark.py \
  --msgs 500 \
  --timeout 180 \
  --label local-1-worker
```

## Benchmark com dois workers por etapa

```bash
docker compose up -d \
  --scale payment=2 \
  --scale stock=2 \
  --scale notification=2
docker compose ps
```

Aguarde todas as réplicas ficarem `healthy`.

```bash
docker compose exec -T dashboard \
  python benchmark.py \
  --msgs 500 \
  --timeout 180 \
  --label local-2-workers
```

## Consultar resultados do benchmark

```bash
docker compose exec -T dashboard \
  python -m json.tool benchmark_results.json
```

## Teste de queda de um worker

Mantenha duas réplicas de cada etapa:

```bash
docker compose up -d \
  --scale payment=2 \
  --scale stock=2 \
  --scale notification=2
docker compose ps
```

Terminal 1:

```bash
docker compose exec -T dashboard \
  python benchmark.py \
  --msgs 2000 \
  --timeout 180 \
  --label falha-payment
```

Terminal 2:

```bash
docker ps \
  --filter label=com.docker.compose.service=payment
```

Copie um `CONTAINER_ID` exibido:

```bash
docker kill --signal KILL CONTAINER_ID
docker compose ps
```

Restaure as réplicas:

```bash
docker compose up -d --scale payment=2
docker compose ps
```

## Teste de reconexão após reiniciar o NATS

Terminal 1:

```bash
docker compose logs -f --tail=0 \
  payment stock notification rpc
```

Terminal 2:

```bash
docker compose restart nats
docker compose ps
```

Depois que o NATS ficar `healthy`:

```bash
docker compose exec -T dashboard \
  python producer.py --total 10 --report 1
docker compose exec -T dashboard python rpc_client.py
```

## Logs e diagnóstico local

```bash
docker compose ps
docker compose logs --tail=100
docker compose logs --tail=100 nats
docker compose logs --tail=100 dashboard
docker compose logs --tail=100 payment stock notification rpc
docker stats --no-stream
```

## Reiniciar serviços locais

```bash
docker compose restart
docker compose ps
```

## Recriar serviços locais

```bash
docker compose up --build --force-recreate -d
docker compose ps
```

## Encerrar o ambiente local

```bash
docker compose down
```

## Execução local sem containers Python

Inicie somente o NATS:

```bash
docker compose up -d nats
```

Prepare o Python:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
export NATS_URL=nats://localhost:4222
export NATS_MONITOR_URL=http://localhost:8222
```

Execute cada comando em um terminal separado:

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
python consumer_payment.py
```

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
python consumer_stock.py
```

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
python consumer_notification.py
```

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
python rpc_server.py
```

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
export NATS_MONITOR_URL=http://localhost:8222
python dashboard.py
```

Testes:

```bash
source venv/bin/activate
export NATS_URL=nats://localhost:4222
python producer.py --total 10 --report 1
python rpc_client.py
python benchmark.py --msgs 500 --timeout 180 --label sem-docker
```

## Implantação em duas VMs

### VM 1 — NATS e dashboard

```bash
git clone -b fix/nats-core-local-stability \
  https://github.com/FelipeGDSZ/TPSD.git
cd TPSD
cp deploy/.env.example deploy/.env
nano deploy/.env
```

Defina no arquivo:

```dotenv
NATS_BIND_IP=IP_PRIVADO_VM_1
DASHBOARD_BIND_IP=0.0.0.0
```

Valide e inicie:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml config --quiet
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml up --build -d
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml ps
```

### VM 2 — workers

```bash
git clone -b fix/nats-core-local-stability \
  https://github.com/FelipeGDSZ/TPSD.git
cd TPSD
cp deploy/.env.example deploy/.env
nano deploy/.env
```

Defina no arquivo:

```dotenv
NATS_URL=nats://IP_PRIVADO_VM_1:4222
PROCESS_LOG_EVERY=100
```

Teste a comunicação antes de iniciar:

```bash
python3 -c "import socket; socket.create_connection(('IP_PRIVADO_VM_1', 4222), 3); print('NATS acessível')"
```

Valide e inicie:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml config --quiet
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up --build -d
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

## Teste E2E nas duas VMs

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml logs -f --tail=0 \
  payment stock notification
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec -T dashboard \
  python producer.py --total 10 --report 1
```

## Teste RPC nas duas VMs

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec -T dashboard \
  python rpc_client.py
```

## Benchmark nas duas VMs com um worker

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=1 \
  --scale stock=1 \
  --scale notification=1
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec -T dashboard \
  python benchmark.py \
  --msgs 500 \
  --timeout 180 \
  --label vm-1-worker
```

## Benchmark nas duas VMs com dois workers

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=2 \
  --scale stock=2 \
  --scale notification=2
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec -T dashboard \
  python benchmark.py \
  --msgs 500 \
  --timeout 180 \
  --label vm-2-workers
```

## Queda de worker nas duas VMs

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml exec -T dashboard \
  python benchmark.py \
  --msgs 2000 \
  --timeout 180 \
  --label vm-falha-payment
```

Durante o benchmark, na VM 2:

```bash
docker ps \
  --filter label=com.docker.compose.project=tpsd-workers \
  --filter label=com.docker.compose.service=payment
docker kill --signal KILL CONTAINER_ID
```

Restaure:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d --scale payment=2
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

## Atualizar o projeto nas VMs

```bash
cd ~/TPSD
git switch fix/nats-core-local-stability
git pull --ff-only
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml up --build -d
```

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up --build -d
```

## Parar os serviços nas VMs

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml stop
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml stop
```

## Retomar os serviços nas VMs

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml up -d
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml ps
```

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml up -d \
  --scale payment=2 \
  --scale stock=2 \
  --scale notification=2
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml ps
```

## Encerrar a implantação nas VMs

VM 2:

```bash
docker compose --env-file deploy/.env \
  -f deploy/workers.compose.yml down
```

VM 1:

```bash
docker compose --env-file deploy/.env \
  -f deploy/nats-dashboard.compose.yml down
```

## Roteiro da apresentação

Consulte `ROTEIRO.md`.
