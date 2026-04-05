from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'lab4',
    bootstrap_servers='broker:9092',
    group_id='enrich-group-1',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Konsument wzbogacający uruchomiony. Szacuję ryzyko...")

for message in consumer:
    tx = message.value
    amount = tx['amount']
    
    if amount > 3000:
        tx['risk_level'] = 'HIGH'
    elif amount > 1000:
        tx['risk_level'] = 'MEDIUM'
    else:
        tx['risk_level'] = 'LOW'
    
    print(f"ID: {tx['tx_id']} | Kwota: {amount:>8.2f} | Ryzyko: {tx['risk_level']}")
