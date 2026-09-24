# Roteiro de Apresentação - TP01 Sistemas Distribuídos

Este é o seu guia passo a passo para o dia da apresentação. Siga estas etapas para ligar a infraestrutura na AWS e fazer a demonstração do sistema.

---

## 1. Ligando o Servidor na AWS
Como a máquina fica desligada para não consumir horas, o primeiro passo é ligá-la:
1. Acesse o painel da **AWS EC2**.
2. Selecione a sua instância (`t3.small`).
3. Clique em **Estado da instância** (Lá em cima) -> **Iniciar instância**.
4. Aguarde o estado ficar **Verde** (`Executando`).
5. Copie o **Endereço IPv4 público** (ex: `18.220.xx.xx`).

---

## 2. Subindo a Infraestrutura
Com a máquina ligada, conecte-se a ela para rodar o sistema:
1. Clique no botão **Conectar** e abra a aba "EC2 Instance Connect" (a tela preta).
2. Entre na pasta do projeto digitando e apertando Enter:
   ```bash
   cd TP-SD/TP1
   ```
3. Suba o cluster do NATS (os 3 servidores):
   ```bash
   sudo docker compose up -d
   ```
4. Verifique se os 3 nós estão rodando:
   ```bash
   sudo docker ps
   ```

---

## 3. Iniciando o Dashboard Web
No mesmo terminal, rode o comando para ligar a interface visual:
```bash
python3 dashboard.py
```
*(Deixe esse terminal aberto. Ele precisa continuar rodando!)*

---

## 4. Mostrando para o Professor
Agora você vai abrir os sites para apresentar:
1. **API de Monitoramento do NATS:** Abra uma nova aba no navegador e acesse `http://SEU_IP_PUBLICO:8222/varz`. 
   - Mostra as métricas em JSON (mensagens in/out, conexões, etc.)
2. **Seu Dashboard Interativo:** Abra outra aba e acesse `http://SEU_IP_PUBLICO:5000`.

---

## 5. Roteiro da Demonstração (O que falar e fazer)

### Cena 1: Produção de Mensagens
- No seu Dashboard, vá em **Produzir Mensagens**.
- Mande gerar mensagens para os subjects de **Pagamento**, **Estoque** ou **Notificação**.
- **O que mostrar:** Mostre as mensagens sendo processadas em tempo real no seu dashboard e no monitor do NATS.

### Cena 2: Os Consumidores e Queue Groups
- Abra terminais separados e inicie os consumidores:
  ```bash
  python3 consumer_payment.py
  python3 consumer_stock.py
  python3 consumer_notification.py
  ```
- **O que mostrar:** Explique que o NATS distribui as mensagens entre múltiplos consumidores usando **Queue Groups** (balanceamento de carga automático).

### Cena 3: Tolerância a Falhas (Queda de Servidor)
- Abra um **Segundo Terminal** na AWS (clicando em "Conectar" de novo em outra aba).
- Force a queda do nó 2:
  ```bash
  sudo docker stop nats2
  ```
- **O que mostrar:**
  1. Vá na API do NATS (`http://SEU_IP:8222/varz`) e mostre que o cluster ainda responde.
  2. Volte no seu Dashboard e mande produzir/consumir mais mensagens.
  3. Explique que o sistema **NÃO PAROU** porque os outros nós do cluster (`nats1` e `nats3`) continuam operacionais.
- Ligue de volta para mostrar a recuperação:
  ```bash
  sudo docker start nats2
  ```

### Cena 4: Comunicação Síncrona (RPC)
- Inicie o servidor RPC de estoque:
  ```bash
  python3 rpc_server.py
  ```
- Em outro terminal, consulte o estoque:
  ```bash
  python3 rpc_client.py
  ```
- **O que mostrar:** Demonstre a consulta síncrona (request-reply) para verificar disponibilidade de produtos em tempo real.

---

## Dica Extra: Como zerar o sistema
Se você precisar limpar o histórico e zerar tudo para começar a apresentação do zero:
1. Aperte `Ctrl + C` para parar o `dashboard.py`.
2. Delete o cluster:
   ```bash
   sudo docker compose down -v
   ```
3. Suba tudo novamente:
   ```bash
   sudo docker compose up -d
   ```
4. Ligue o dashboard novamente (`python3 dashboard.py`) e tudo estará zerado!
