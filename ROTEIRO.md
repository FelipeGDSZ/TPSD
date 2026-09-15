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
3. Suba o cluster do RabbitMQ (os 3 servidores):
   ```bash
   sudo docker compose up -d
   ```
4. Aplique a configuração de cluster e filas resilientes (Quorum Queues):
   ```bash
   sudo bash init_cluster.sh
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
1. **Painel do RabbitMQ:** Abra uma nova aba no navegador e acesse `http://SEU_IP_PUBLICO:15672`. 
   - Usuário: `admin` | Senha: `admin123`
2. **Seu Dashboard Interativo:** Abra outra aba e acesse `http://SEU_IP_PUBLICO:5000`.

---

## 5. Roteiro da Demonstração (O que falar e fazer)

### Cena 1: Produção de Mensagens
- No seu Dashboard, vá em **Produzir Mensagens**.
- Mande gerar `1000` mensagens para a fila de **Estoque**.
- **O que mostrar:** Mostre o número de mensagens crescendo instantaneamente no seu mapa e no gráfico original do RabbitMQ.

### Cena 2: O Consumidor e o QoS
- Vá na caixa **Consumir Mensagens**.
- Peça para consumir `1000` mensagens da fila de **Estoque**.
- **O que mostrar:** Explique que o sistema está puxando `1 mensagem por vez` e mostre o número caindo gradativamente enquanto a "estrelinha" amarela aparece.

### Cena 3: Tolerância a Falhas (Queda de Servidor)
- Abra um **Segundo Terminal** na AWS (clicando em "Conectar" de novo em outra aba).
- Force a queda do nó 2:
  ```bash
  sudo docker stop rabbit2
  ```
- **O que mostrar:**
  1. Vá no painel do RabbitMQ (porta 15672) e mostre que o `rabbit2` caiu (ficou vermelho).
  2. Volte no seu Dashboard e mande produzir/consumir mais mensagens.
  3. Explique que o sistema **NÃO PAROU** porque as *Quorum Queues* garantem cópias da fila nos nós sobreviventes (`rabbit1` e `rabbit3`).
- Ligue de volta para mostrar a recuperação:
  ```bash
  sudo docker start rabbit2
  ```

---

## Dica Extra: Como zerar as filas e o sistema
Se você precisar limpar o histórico de mensagens, consumidores e zerar as filas para começar a apresentação do zero, siga os passos abaixo (leva 10 segundos):
1. Aperte `Ctrl + C` para parar o `dashboard.py`.
2. Delete o cluster e os volumes:
   ```bash
   sudo docker compose down -v
   ```
3. Suba tudo novamente:
   ```bash
   sudo docker compose up -d
   ```
4. Recrie as filas seguras:
   ```bash
   sudo bash init_cluster.sh
   ```
5. Ligue o dashboard novamente (`python3 dashboard.py`) e tudo estará 100% zerado!
