import json
import sqlite3
from kafka import KafkaConsumer, KafkaProducer

# 1. Configura o banco SQLite
conn = sqlite3.connect('estoque.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS produtos (
        id INTEGER PRIMARY KEY,
        nome TEXT NOT NULL,
        quantidade INTEGER NOT NULL
    )
''')

# Popula dados iniciais apenas se a tabela estiver vazia
cursor.execute('SELECT COUNT(*) FROM produtos')
if cursor.fetchone()[0] == 0:
    cursor.executemany(
        'INSERT INTO produtos (id, nome, quantidade) VALUES (?, ?, ?)',
        [(1, 'Notebook', 3), (2, 'Mouse', 1)]
    )
    conn.commit()
    print("Banco SQLite inicializado: Notebook (qtd: 3), Mouse (qtd: 1)")

# 2. Configura Kafka
consumer = KafkaConsumer(
    'pedidos.criados',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-estoque',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de Estoque aguardando pedidos... ===")

try:
    for msg in consumer:
        pedido = msg.value
        pedido_id = pedido['pedido_id']
        produto_id = pedido['produto_id']
        qtd = pedido['quantidade']

        print(f"\n[Recebido] Verificando pedido {pedido_id} (Produto {produto_id}, Qtd: {qtd})...")

        # Atualização atômica: só desconta se houver saldo suficiente
        cursor.execute(
            'UPDATE produtos SET quantidade = quantidade - ? WHERE id = ? AND quantidade >= ?',
            (qtd, produto_id, qtd)
        )
        conn.commit()

        if cursor.rowcount > 0:
            print(f"[OK] Estoque reservado para o pedido {pedido_id}!")
            producer.send('estoque.reservado', key=pedido_id, value=pedido)
        else:
            print(f"[FALHA] Estoque insuficiente ou produto inexistente para o pedido {pedido_id}.")
            evento_cancelado = {**pedido, "motivo": "Sem estoque disponível"}
            producer.send('pedido.cancelado', key=pedido_id, value=evento_cancelado)

        producer.flush()

except KeyboardInterrupt:
    print("\nEncerrando Serviço de Estoque...")
finally:
    consumer.close()
    producer.close()
    conn.close()