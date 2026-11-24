import json
from kafka import KafkaConsumer

# Passando 3 tópicos ao mesmo tempo para o mesmo consumidor!
consumer = KafkaConsumer(
    'pedido.cancelado',
    'nf.emitida',
    'produto.enviado',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-notificacoes',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("=== Serviço de Notificações (E-mail / WhatsApp) aguardando... ===")

try:
    for msg in consumer:
        dados = msg.value
        pedido_id = dados['pedido_id']
        topico = msg.topic  # Descobre de qual tópico veio essa mensagem

        if topico == 'pedido.cancelado':
            motivo = dados.get('motivo', 'Motivo não informado')
            print(f"\n[WHATSAPP] Olá! Infelizmente o pedido {pedido_id} foi cancelado ({motivo}).")

        elif topico == 'nf.emitida':
            nf = dados['nota_fiscal']
            print(f"\n[E-MAIL] Boa notícia! A Nota Fiscal {nf} do pedido {pedido_id} já foi emitida.")

        elif topico == 'produto.enviado':
            rastreio = dados['rastreio']
            print(f"\n[WHATSAPP] Oba! Seu pedido {pedido_id} foi despachado! Rastreio: {rastreio}")

except KeyboardInterrupt:
    print("\nEncerrando Serviço de Notificações...")
finally:
    consumer.close()