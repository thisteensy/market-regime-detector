#!/usr/bin/env python3
import json
from kafka import KafkaConsumer
import uuid

consumer = KafkaConsumer(
    'fx-rates',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    consumer_timeout_ms=2000,
    group_id=f'debug-{uuid.uuid4()}'
)

print("Reading 3 fx-rates messages...\n")

count = 0
for message in consumer:
    count += 1
    print(f"Message {count}:")
    print(json.dumps(message.value, indent=2))
    print()
    if count >= 3:
        break

