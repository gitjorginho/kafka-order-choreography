# Kafka Order Choreography

Projeto prático de **Arquitetura Orientada a Eventos (EDA)** utilizando **Apache Kafka**, **Python** e **SQLite**, simulando o fluxo assíncrono de pedidos de um e-commerce com o padrão de **Coreografia**.

## Arquitetura e Fluxo

Cada microsserviço consome eventos de um tópico, executa sua responsabilidade e publica um novo evento para o próximo estágio, de forma totalmente desacoplada:

```text
[1. API Pedidos] ──(pedidos.criados)──▶ [2. Estoque + SQLite]
                                              │
                 ┌───(pedido.cancelado)───────┤ (sem estoque)
                 │                            │ (estoque reservado)
                 ▼                            ▼
      [5. Notificações] ◀──(nf.emitida)── [3. Nota Fiscal]
               ▲                              │
               │                              ▼
               └───(produto.enviado)── [4. Transportadora]
```

### Serviços
1. **`1_api_pedidos.py`**: Recebe os pedidos e publica no tópico `pedidos.criados` utilizando o `pedido_id` como chave de particionamento.
2. **`2_servico_estoque.py`**: Consome `pedidos.criados`, verifica e reserva o saldo de forma atômica no SQLite (`estoque.db`). Publica em `estoque.reservado` ou `pedido.cancelado`.
3. **`3_servico_nf.py`**: Consome `estoque.reservado`, simula a emissão da Nota Fiscal e publica em `nf.emitida`.
4. **`4_servico_transportadora.py`**: Consome `nf.emitida`, gera o código de rastreio e publica em `produto.enviado`.
5. **`5_notificacoes.py`**: Exemplo de serviço plugado sem alterar os anteriores. Escuta múltiplos tópicos (`pedido.cancelado`, `nf.emitida` e `produto.enviado`) usando um `group_id` independente.

## Tecnologias
* Python 3
* Apache Kafka (`kafka-python`)
* SQLite (`sqlite3`)

## Como executar

1. Certifique-se de que o **Apache Kafka** está rodando em `localhost:9092`.

2. Crie e ative o ambiente virtual e instale as dependências:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Em terminais separados (com a `.venv` ativada), execute os consumidores:
```bash
python3 2_servico_estoque.py
python3 3_servico_nf.py
python3 4_servico_transportadora.py
python3 5_notificacoes.py
```

4. Em outro terminal, inicie a API para enviar pedidos:
```bash
python3 1_api_pedidos.py
```