import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'nf.emitida',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-transportadora',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=== Serviço de Transportadora aguardando... ===")

try:
    for msg in consumer:
        dados = msg.value
        pedido_id = dados['pedido_id']
        nf = dados['nota_fiscal']

        print(f"\n[Recebido] Preparando envio do pedido {pedido_id} (NF: {nf})...")
        time.sleep(1.5)  # Simula geração de etiqueta / API da transportadora

        codigo_rastreio = f"BR{str(uuid.uuid4())[:8].upper()}"
        evento_envio = {
            **dados,
            "rastreio": codigo_rastreio,
            "status": "DESPACHADO"
        }

        producer.send('produto.enviado', key=pedido_id, value=evento_envio)
        producer.flush()
        print(f"[CONCLUÍDO] Pedido {pedido_id} despachado! Rastreio: {codigo_rastreio}")

except KeyboardInterrupt:
    print("\nEncerrando Serviço de Transportadora...")
finally:
    consumer.close()
    producer.close()