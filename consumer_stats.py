from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'lab4', 
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

cat_counts = defaultdict(int)
cat_revenue = defaultdict(float)
cat_min = {}
cat_max = {}
msg_count = 0

print("Konsument statystyk kategorii uruchomiony...")

try:
    for message in consumer:
        data = message.value
        cat = data['category']
        amount = data['amount']
        
        cat_counts[cat] += 1
        cat_revenue[cat] += amount
        
        if cat not in cat_min or amount < cat_min[cat]:
            cat_min[cat] = amount
        if cat not in cat_max or amount > cat_max[cat]:
            cat_max[cat] = amount
            
        msg_count += 1
        
        if msg_count % 10 == 0:
            print(f"\n--- RAPORT KATEGORII (po {msg_count} wiadomościach) ---")
            print(f"{'Kategoria':<15} | {'Liczba':<5} | {'Suma':<10} | {'Min':<8} | {'Max':<8}")
            print("-" * 65)
            for c in cat_counts:
                print(f"{c:<15} | {cat_counts[c]:<5} | {cat_revenue[c]:<10.2f} | {cat_min[c]:<8.2f} | {cat_max[c]:<8.2f}")
except KeyboardInterrupt:
    print("Zatrzymano.")
