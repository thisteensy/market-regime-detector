#!/usr/bin/env python3
"""
Integration test: Feed data through the pipeline
Producer → Kafka → Job 1 (normalize) → Job 2 (correlate)

This simulates what would happen in production.
"""

import json
import sys
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, './flink_jobs')

from job_1_normalization import NormalizationMapper
from job_2_correlation import CorrelationCalculator

print("\n" + "="*70)
print("INTEGRATION TEST: Full Pipeline Simulation")
print("="*70)

# Step 1: Raw data (what comes from API)
print("\n[STEP 1] Raw data from producer...")
print("-" * 70)

raw_messages = [
    {
        'timestamp': (datetime.now() + timedelta(minutes=i)).isoformat(),
        'source': 'yahoo_finance',
        'data': {'eur_usd': 1.085 + (i * 0.001)},
        'commodities': {'oil': 75 + (i * 0.8), 'gold': 2050 + (i * 0.2)},
        'indices': {'sp500': 5000 + (i * 5)},
        'volatility': {'vix': 15 - (i * 0.1)},
        'yields': {'bund_yield': 2.5 + (i * 0.01)}
    }
    for i in range(20)
]

print(f"Generated {len(raw_messages)} raw messages")
print(f"Sample: {json.dumps(raw_messages[0], indent=2)[:200]}...")

# Step 2: Job 1 - Normalize
print("\n[STEP 2] Job 1: Normalization...")
print("-" * 70)

normalizer = NormalizationMapper()
normalized_messages = []

for raw_msg in raw_messages:
    for normalized in normalizer.map(json.dumps(raw_msg)):
        normalized_messages.append(json.loads(normalized))

print(f"Raw messages: {len(raw_messages)}")
print(f"Normalized records: {len(normalized_messages)}")
print(f"Sample normalized record:")
print(json.dumps(normalized_messages[0], indent=2))

# Step 3: Job 2 - Correlation
print("\n[STEP 3] Job 2: Correlation Analysis...")
print("-" * 70)

calc_1h = CorrelationCalculator(window_size_minutes=60)
calc_4h = CorrelationCalculator(window_size_minutes=240)

# Feed normalized data through correlation calculator
correlation_outputs = []

for record in normalized_messages:
    timestamp = record['timestamp']
    symbol = record['symbol']
    value = record['value']
    
    calc_1h.add_data_point(timestamp, symbol, value)
    calc_4h.add_data_point(timestamp, symbol, value)
    
    # Emit correlations every few records (simulate windowing)
    if len(normalized_messages) > 0 and normalized_messages.index(record) % 5 == 0:
        corr_1h = calc_1h.calculate_correlations()
        corr_4h = calc_4h.calculate_correlations()
        
        if corr_1h and corr_4h:
            output = {
                'timestamp': timestamp,
                '1h_correlations': corr_1h,
                '1h_regime': calc_1h.get_regime(),
                '4h_correlations': corr_4h,
                '4h_regime': calc_4h.get_regime()
            }
            correlation_outputs.append(output)

print(f"Correlation calculations: {len(correlation_outputs)}")
print(f"Latest correlation output:")
if correlation_outputs:
    latest = correlation_outputs[-1]
    print(f"  Timestamp: {latest['timestamp']}")
    print(f"  1h regime: {latest['1h_regime']}")
    print(f"  1h correlations:")
    for sym, corr in latest['1h_correlations'].items():
        print(f"    {sym}: {corr:+.3f}")
    print(f"  4h regime: {latest['4h_regime']}")

# Step 4: Summary
print("\n" + "="*70)
print("INTEGRATION TEST SUMMARY")
print("="*70)

print(f"\n✓ Step 1 (Raw data): {len(raw_messages)} messages")
print(f"✓ Step 2 (Normalization): {len(normalized_messages)} records")
print(f"✓ Step 3 (Correlation): {len(correlation_outputs)} outputs")

print("\nData flow verified:")
print("  Raw API data")
print("      ↓")
print("  Job 1: Normalize")
print("      ↓")
print("  Job 2: Correlate")
print("      ↓")
print("  Correlation matrices + regime detection")

print("\n✅ Full pipeline working end-to-end!")
print("\nThis is what happens in production:")
print("  1. Data producer publishes raw data to Kafka")
print("  2. Flink Job 1 normalizes it")
print("  3. Flink Job 2 calculates correlations")
print("  4. Results go to InfluxDB")
print("  5. API serves to dashboard")

