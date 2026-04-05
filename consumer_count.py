from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'lab4', 
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Konsument uruchomiony. Czekam na transakcje...")

try:
    for message in consumer:
        data = message.value
        store = data['store']
        amount = data['amount']
        
        store_counts[store] += 1
        total_amount[store] = total_amount.get(store, 0) + amount
        msg_count += 1
        
        if msg_count % 10 == 0:
            print(f"\nPodsumowanie po {msg_count} wiadomościach")
            for s in store_counts:
                cnt = store_counts[s]
                suma = total_amount[s]
                print(f"Sklep: {s:<10} | Razem: {suma:>8.2f} PLN | Średnia: {suma/cnt:>7.2f} PLN")
except KeyboardInterrupt:
    print("Zatrzymano konsumenta.")
