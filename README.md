# TP1 Sistemas Distribuídos - NATS

Este repositório contém o Trabalho Prático 1 da disciplina de Sistemas Distribuídos (Ciência da Computação - UFOP).

## Arquitetura do Projeto
* **Producer (`producer.py`):** Gera pedidos fictícios assincronamente e publica no NATS.
* **Workers (`consumer_*.py`):** Consumidores construídos com `asyncio` que utilizam **Queue Groups** do NATS para balanceamento de carga (Pagamento, Estoque, Notificação).
* **Dashboard (`dashboard.py`):** Servidor Flask que consome a API HTTP nativa do NATS (porta 8222) e envia métricas via *Server-Sent Events (SSE)* para um frontend topológico desenhado com Vis.js.
* **Infraestrutura:** Suporte completo a Docker e Docker Swarm para orquestração de contêineres.

## Tecnologias Utilizadas
* Python 3.12+ (Bibliotecas: `nats-py`, `flask`, `requests`, `matplotlib`)
* NATS Server (Docker)
* HTML5 + Tailwind CSS + Vis.js (Frontend)

## Como executar localmente

1. **Suba o cluster NATS:**

   docker-compose up -d
   
2. Crie e ative o ambiente visual:

   python3 -m venv venv

   source venv/bin/activate

   No Windows use: venv\Scripts\activate

3. Inicie o monitoramento:

   python dashboard.py

4. Acesse http://localhost:5000 no seu navegador.

5. Inicie os Workers (Abra terminais separados e ative o venv em todos):

   python consumer_payment.py

   python consumer_stock.py

   python consumer_notification.py

   Disparo de Carga (Producer): python producer.py --total 10000

6. Execute o Case RPC

   Mantenha o NATS rodando, abra dois novos terminais (com o venv ativo) e observe a comunicação síncrona:

   Servidor de Estoque: python rpc_server.py

   Cliente de Consulta: python rpc_client.py

