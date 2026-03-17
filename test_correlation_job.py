#!/usr/bin/env python3
"""
Test Job 2 correlation logic with mock data.
Proves the math works before deploying to Flink.
"""

import sys
sys.path.insert(0, './flink_jobs')

from job_2_correlation import CorrelationCalculator
import json
from datetime import datetime, timedelta

# Create calculator
calc = CorrelationCalculator(window_size_minutes=60)

print("\n" + "="*60)
print("Testing Correlation Calculator")
print("="*60)

# Generate 30 minutes of correlated mock data
base_time = datetime.now()

print("\nAdding data points...")

# EUR/USD and Oil are positively correlated (0.8+)
# Gold is uncorrelated (0.0)
# VIX is negatively correlated (-0.6)

for i in range(30):
    timestamp = (base_time + timedelta(minutes=i)).isoformat()
    
    # EUR/USD base
    eur_val = 1.085 + (i * 0.001) + (0.002 * (i % 5))
    calc.add_data_point(timestamp, 'eur_usd', eur_val)
    
    # Oil: moves WITH EUR/USD (positive correlation)
    oil_val = 75.0 + (i * 0.8) + (1.5 * (i % 5))
    calc.add_data_point(timestamp, 'oil', oil_val)
    
    # Gold: random (uncorrelated)
    gold_val = 2050 + (i * 0.2) - (5 * (i % 3))
    calc.add_data_point(timestamp, 'gold', gold_val)
    
    # VIX: moves OPPOSITE EUR/USD (negative correlation)
    vix_val = 15.0 - (i * 0.15) + (0.5 * (i % 4))
    calc.add_data_point(timestamp, 'vix', vix_val)
    
    if (i + 1) % 10 == 0:
        print(f"  Added {i + 1} data points...")

print("\n✓ Data loaded")

# Calculate correlations
print("\nCalculating correlations...")
correlations = calc.calculate_correlations()

print("\nResults:")
print("-" * 60)
for symbol, corr in sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True):
    direction = "↑" if corr > 0 else "↓" if corr < 0 else "→"
    bar = "█" * int(abs(corr) * 20)
    print(f"  {symbol:12} {direction} {corr:+.3f}  {bar}")

# Detect regime
regime = calc.get_regime()
print(f"\nMarket Regime: EUR/USD is following {regime}")

print("\n" + "="*60)
print("Interpretation:")
print("="*60)
print(f"  Oil: {correlations.get('oil', 0):+.3f}  (should be ~+0.8, high positive)")
print(f"  Gold: {correlations.get('gold', 0):+.3f}  (should be ~0.0, uncorrelated)")
print(f"  VIX: {correlations.get('vix', 0):+.3f}  (should be ~-0.6, negative)")
print("\n✓ If these match expectations, the correlation math works!")

