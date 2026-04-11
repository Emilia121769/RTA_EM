from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict

consumer = KafkaConsumer(
    'lab4',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

user_history = defaultdict(list)

print("Detektor anomalii uruchomiony. Szukam > 3 transakcji w 60s...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    
    current_tx_time = datetime.fromisoformat(tx['timestamp'])
    user_history[user_id].append(current_tx_time)
    
    time_limit = current_tx_time - timedelta(seconds=60)
    user_history[user_id] = [t for t in user_history[user_id] if t > time_limit]
    
    tx_count = len(user_history[user_id])
    if tx_count > 3:
        print(f"!!! ANOMALIA !!! User {user_id} zrobił {tx_count} transakcje w 60s! (Kwota: {tx['amount']} PLN)")
