#!/bin/bash
# init_cluster.sh
# Une os 3 nós RabbitMQ em cluster e ativa Quorum Queues
# Execute após: docker compose up -d
# Aguarde ~30 segundos para os nós inicializarem antes de rodar este script

set -e

echo "==> Aguardando rabbit1 ficar pronto..."
until docker exec rabbit1 rabbitmqctl status > /dev/null 2>&1; do
  sleep 2
done

echo "==> Aguardando rabbit2 ficar pronto..."
until docker exec rabbit2 rabbitmqctl status > /dev/null 2>&1; do
  sleep 2
done

echo "==> Aguardando rabbit3 ficar pronto..."
until docker exec rabbit3 rabbitmqctl status > /dev/null 2>&1; do
  sleep 2
done

echo ""
echo "==> Unindo rabbit2 ao cluster..."
docker exec rabbit2 rabbitmqctl stop_app
docker exec rabbit2 rabbitmqctl reset
docker exec rabbit2 rabbitmqctl join_cluster rabbit@rabbit1
docker exec rabbit2 rabbitmqctl start_app

echo "==> Unindo rabbit3 ao cluster..."
docker exec rabbit3 rabbitmqctl stop_app
docker exec rabbit3 rabbitmqctl reset
docker exec rabbit3 rabbitmqctl join_cluster rabbit@rabbit1
docker exec rabbit3 rabbitmqctl start_app

echo ""
echo "==> Status do cluster:"
docker exec rabbit1 rabbitmqctl cluster_status

echo ""
echo "==> Ativando Quorum Queues como padrao..."
docker exec rabbit1 rabbitmqctl set_policy quorum-policy "^orders\." \
  "{\"queue-mode\":\"lazy\"}" --apply-to queues --priority 1

echo ""
echo "Cluster pronto! Acesse o Management UI em: http://localhost:15672"
echo "Usuario: admin | Senha: admin123"
