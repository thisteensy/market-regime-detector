#!/usr/bin/env python3
import sys
sys.path.insert(0, './flink_jobs')
from job_2_correlation import CorrelationCalculator
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import uuid

# Create calculator
calc = CorrelationCalculator(window_size_minutes=60)

# Read from Kafka
consumer = KafkaConsumer(
    'fx-rates', 'commodities', 'indices', 'volatility', 'yields',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    consumer_timeout_ms=1000,
    group_id=f'debug-{uuid.uuid4()}'
)

print("Reading messages and calculating correlations...\n")

count = 0
for message in consumer:
    data = message.value
    timestamp = data.get('timestamp', datetime.utcnow().isoformat())
    
    # Extract data
    for key in ['data', 'commodities', 'indices', 'volatility', 'yields']:
        if key in data and isinstance(data[key], dict):
            for symbol, value in data[key].items():
                if symbol != 'timestamp':
                    try:
                        calc.add_data_point(timestamp, symbol, float(value))
                    except:
                        pass
    
    count += 1
    
    if count % 20 == 0:
        corrs = calc.calculate_correlations()
        print(f"After {count} messages:")
        print(f"  Buffer sizes: eur_usd={len(calc.data_buffer.get('eur_usd', []))}, oil={len(calc.data_buffer.get('oil', []))}")
        print(f"  Correlations: {corrs}")
        print()
        
        if count >= 100:
            break

