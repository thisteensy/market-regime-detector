#!/usr/bin/env python3
"""Flask API to serve correlation data to the web dashboard"""

import sys
import json
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

sys.path.insert(0, './analytics')
from market_regime_detector.analytics.job_2_correlation import CorrelationCalculator

from kafka import KafkaConsumer
import uuid
import threading

app = Flask(__name__)
CORS(app)

# Global state with multiple calculators for different windows
state = {
    'correlations': {},
    'regime': 'WAITING',
    'prices': {},
    'messages_received': 0,
    'last_update': None
}

# Multiple calculators for different windows
calculators = {
    360: CorrelationCalculator(window_size_minutes=360),    # 6h
    720: CorrelationCalculator(window_size_minutes=720),    # 12h
    1440: CorrelationCalculator(window_size_minutes=1440),  # 1d
    10080: CorrelationCalculator(window_size_minutes=10080) # 1w
}

def consume_kafka():
    """Background thread to consume Kafka messages"""
    print("Starting Kafka consumer thread...")
    try:
        consumer = KafkaConsumer(
            'fx-rates', 'commodities', 'indices', 'volatility', 'yields',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=500,
            group_id=f'api-{uuid.uuid4()}'
        )
        
        print("Connected to Kafka, listening for messages...")
        
        while True:
            message = consumer.poll(timeout_ms=500)
            
            if message:
                for topic_partition, records in message.items():
                    for record in records:
                        data = record.value
                        timestamp = data.get('timestamp', datetime.utcnow().isoformat())
                        
                        # Extract data
                        for key in ['data', 'commodities', 'indices', 'volatility', 'yields']:
                            if key in data and isinstance(data[key], dict):
                                for symbol, value in data[key].items():
                                    if symbol != 'timestamp':
                                        try:
                                            val = float(value)
                                            state['prices'][symbol] = val
                                            # Add to all calculators
                                            for calc in calculators.values():
                                                calc.add_data_point(timestamp, symbol, val)
                                        except (ValueError, TypeError):
                                            pass
                        
                        state['messages_received'] += 1
                        state['last_update'] = datetime.now().isoformat()
                        
                        if state['messages_received'] % 50 == 0:
                            buffer_size = len(calculators[1440].data_buffer.get('eur_usd', []))
                            print(f"📊 Processed {state['messages_received']} messages, buffer: {buffer_size}")
    
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        import traceback
        traceback.print_exc()

@app.route('/api/correlations', methods=['GET'])
def get_correlations():
    """Return current correlation data for specified window"""
    window = request.args.get('window', 1440, type=int)
    
    # Validate window
    if window not in calculators:
        window = 1440
    
    calc = calculators[window]
    correlations = calc.calculate_correlations()
    regime = calc.get_regime()
    
    return jsonify({
        'correlations': correlations,
        'regime': regime,
        'prices': state['prices'],
        'messages_received': state['messages_received'],
        'last_update': state['last_update'],
        'window_minutes': window,
        'buffer_size': len(calc.data_buffer.get('eur_usd', []))
    })

@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'ok',
        'connected': state['messages_received'] > 0,
        'messages_received': state['messages_received']
    })

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    thread = threading.Thread(target=consume_kafka, daemon=True)
    thread.start()
    
    print("🚀 API server starting on http://localhost:5001")
    app.run(debug=False, port=5001, threaded=True)
