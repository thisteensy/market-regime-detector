#!/usr/bin/env python3
import sys
sys.path.insert(0, './flink_jobs')
from job_2_correlation import CorrelationCalculator
from datetime import datetime, timedelta

calc = CorrelationCalculator(window_size_minutes=60)

# Add 70 data points with current timestamps
base_time = datetime.now()
for i in range(70):
    timestamp = (base_time - timedelta(minutes=70-i)).isoformat()
    
    # Correlated data
    eur = 1.09 + (i * 0.001)
    oil = 70 + (i * 0.5)
    gold = 1500 + (i * 2)
    
    calc.add_data_point(timestamp, 'eur_usd', eur)
    calc.add_data_point(timestamp, 'oil', oil)
    calc.add_data_point(timestamp, 'gold', gold)

# Check buffer
print(f"EUR/USD buffer: {len(calc.data_buffer['eur_usd'])}")
print(f"Oil buffer: {len(calc.data_buffer['oil'])}")

# Calculate
corrs = calc.calculate_correlations()
print(f"\nCorrelations: {corrs}")

