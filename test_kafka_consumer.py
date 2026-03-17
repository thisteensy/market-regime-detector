#!/usr/bin/env python3
"""Quick test: Can we read from Kafka?"""

from kafka import KafkaConsumer
import json
import uuid

print("Connecting to Kafka...")

try:
    # Use a unique group ID each time
    group_id = f"test-consumer-{uuid.uuid4()}"
    
    consumer = KafkaConsumer(
        'fx-rates',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        group_id=group_id
    )
    
    print(f"✓ Connected with group: {group_id}")
    print("Reading messages...")
    
    count = 0
    for message in consumer:
        data = json.loads(message.value)
        print(f"Message {count}: {data}")
        count += 1
        if count >= 3:
            break
    
    print(f"✓ Read {count} messages")
    consumer.close()

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

