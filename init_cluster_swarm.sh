#!/bin/bash
# init_cluster_swarm.sh
# ─────────────────────────────────────────────────────────────────────
# Une os 3 nós RabbitMQ do Swarm em um único cluster replicado.
#
# Diferença em relação ao init_cluster.sh (docker-compose):
#   No docker-compose, os containers têm nomes fixos (rabbit1, rabbit2, rabbit3).
#   No Swarm, os containers ganham nomes gerados automaticamente no formato:
#       tp01_rabbit1.1.xxxxxxxxxxxxxxxx
#   Por isso, este script usa "docker ps --filter" para encontrar
#   o ID real de cada container antes de executar os comandos.
#
# Execute APÓS:
#   docker stack deploy -c docker-stack.yml tp01
#
# Aguarde ~30 segundos para os containers inicializarem antes de rodar.

set -e

echo ""
echo "==> Localizando os containers do Swarm..."

# Busca o ID do container de cada nó pelo nome do serviço
RABBIT1=$(docker ps --filter "name=tp01_rabbit1" --format "{{.ID}}" | head -1)
RABBIT2=$(docker ps --filter "name=tp01_rabbit2" --format "{{.ID}}" | head -1)
RABBIT3=$(docker ps --filter "name=tp01_rabbit3" --format "{{.ID}}" | head -1)

# Verifica se todos os 3 containers foram encontrados
if [ -z "$RABBIT1" ] || [ -z "$RABBIT2" ] || [ -z "$RABBIT3" ]; then
    echo "[ERRO] Um ou mais containers não foram encontrados."
    echo "       Verifique se o stack está rodando com: docker service ls"
    exit 1
fi

echo "    rabbit1: $RABBIT1"
echo "    rabbit2: $RABBIT2"
echo "    rabbit3: $RABBIT3"

echo ""
echo "==> Aguardando rabbit1 ficar pronto..."
until docker exec "$RABBIT1" rabbitmqctl status > /dev/null 2>&1; do
    echo "    ... ainda iniciando, aguardando 3s..."
    sleep 3
done
echo "    rabbit1 pronto!"

echo ""
echo "==> Aguardando rabbit2 ficar pronto..."
until docker exec "$RABBIT2" rabbitmqctl status > /dev/null 2>&1; do
    sleep 3
done
echo "    rabbit2 pronto!"

echo ""
echo "==> Aguardando rabbit3 ficar pronto..."
until docker exec "$RABBIT3" rabbitmqctl status > /dev/null 2>&1; do
    sleep 3
done
echo "    rabbit3 pronto!"

echo ""
echo "==> Unindo rabbit2 ao cluster do rabbit1..."
docker exec "$RABBIT2" rabbitmqctl stop_app
docker exec "$RABBIT2" rabbitmqctl reset
docker exec "$RABBIT2" rabbitmqctl join_cluster rabbit@rabbit1
docker exec "$RABBIT2" rabbitmqctl start_app

echo ""
echo "==> Unindo rabbit3 ao cluster do rabbit1..."
docker exec "$RABBIT3" rabbitmqctl stop_app
docker exec "$RABBIT3" rabbitmqctl reset
docker exec "$RABBIT3" rabbitmqctl join_cluster rabbit@rabbit1
docker exec "$RABBIT3" rabbitmqctl start_app

echo ""
echo "==> Status final do cluster:"
docker exec "$RABBIT1" rabbitmqctl cluster_status

echo ""
echo "✅ Cluster RabbitMQ configurado com sucesso via Docker Swarm!"
echo "   Acesse o painel em: http://localhost:15672"
echo "   Usuário: admin | Senha: admin123"
