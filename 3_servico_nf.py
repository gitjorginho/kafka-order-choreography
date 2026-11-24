import json
import time
import random
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'estoque.reservado',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-nf',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de Nota Fiscal aguardando... ===")

try:
    for msg in consumer:
        dados = msg.value
        pedido_id = dados['pedido_id']

        print(f"\n[Processando] Emitindo NF-e para o pedido {pedido_id}...")
        time.sleep(1.5)  # Simula tempo de resposta da SEFAZ

        numero_nf = f"NF-{random.randint(10000, 99999)}"
        evento_nf = {
            **dados,
            "nota_fiscal": numero_nf
        }

        producer.send('nf.emitida', key=pedido_id, value=evento_nf)
        producer.flush()
        print(f"[OK] Nota Fiscal {numero_nf} emitida para o pedido {pedido_id}!")

except KeyboardInterrupt:
    print("\nEncerrando Serviço de NF...")
finally:
    consumer.close()
    producer.close()