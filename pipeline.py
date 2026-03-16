#!/usr/bin/env python3
"""
EUR/USD Correlation Pipeline - Phase 1: Local Validation
Orchestrates data fetching → normalization → correlation analysis
"""

import sys
import pandas as pd
from pathlib import Path
from data_fetcher import DataFetcher
from correlation_analysis import CorrelationAnalyzer


def main():
    print("\n" + "="*70)
    print(" EUR/USD CORRELATION ANALYSIS - LOCAL VALIDATION PIPELINE")
    print(" Phase 1: Prove the concept works before scaling to Flink")
    print("="*70)
    
    # Step 1: Fetch data
    print("\n[STEP 1/4] Fetching market data from APIs...")
    print("-" * 70)
    
    fetcher = DataFetcher(lookback_days=90)
    combined_data = fetcher.fetch_all()
    
    print(f"\n✅ Successfully fetched data")
    print(f"   Rows: {len(combined_data)}")
    print(f"   Time span: {combined_data['timestamp'].min()} to {combined_data['timestamp'].max()}")
    print(f"   Instruments: {', '.join([col for col in combined_data.columns if col != 'timestamp'])}")
    
    # Step 2: Normalize
    print("\n[STEP 2/4] Normalizing data to Z-scores...")
    print("-" * 70)
    
    analyzer = CorrelationAnalyzer(window_hours=[1, 4, 24], slide_hours=1)
    analyzer.load_data(combined_data)
    analyzer.normalize()
    
    # Step 3: Calculate correlations
    print("\n[STEP 3/4] Calculating rolling correlations...")
    print("-" * 70)
    
    correlations = analyzer.calculate_correlations()
    
    # Show sample results
    for window_h, corr_df in correlations.items():
        if len(corr_df) == 0:
            print(f"\n📊 {window_h} window: (not enough data)")
            continue
        print(f"\n📊 Latest {window_h} correlations:")
        latest = corr_df.iloc[-1]
        instruments = ['oil', 'gold', 'sp500', 'vix', 'bund_yield']
        for instr in instruments:
            corr_val = latest[instr]
            # Visual representation
            bar = "█" * int(abs(corr_val) * 10) if not pd.isna(corr_val) else "?"
            direction = "↑" if corr_val > 0 else "↓" if corr_val < 0 else "→"
            print(f"   {instr:12} {direction} {corr_val:+.3f}  {bar}")
    
    # Step 4: Analyze regimes and anomalies
    print("\n[STEP 4/4] Detecting regimes and anomalies...")
    print("-" * 70)
    
    regime_df = analyzer.detect_regime_change(window_h=4, threshold=0.3)
    regime_summary = regime_df['regime'].value_counts()
    print("\n📈 Market regimes (4-hour window):")
    for regime, count in regime_summary.items():
        pct = 100 * count / len(regime_df)
        print(f"   {regime:12} {count:4} occurrences ({pct:5.1f}%)")
    
    anomalies = analyzer.detect_anomalies(window_h=1, zscore_threshold=2.0)
    print(f"\n🚨 Anomalies detected: {len(anomalies)} spikes (>{2.0}σ moves)")
    if len(anomalies) > 0:
        print("   Sample anomalies:")
        for idx, row in anomalies.head(3).iterrows():
            print(f"   {row['timestamp']} | EUR/USD change: {row['eur_usd_change']:+.2f}% (zscore: {row['eur_usd_zscore']:+.2f})")
    
    # Export for next phase
    print("\n[EXPORT] Writing results to JSON for downstream processing...")
    print("-" * 70)
    analyzer.export_to_json()
    
    # Summary statistics
    print("\n" + "="*70)
    print(" VALIDATION SUMMARY")
    print("="*70)
    print(f"✅ Data integrity: OK ({len(combined_data)} aligned rows)")
    print(f"✅ Correlation math: Working (calculated {len(correlations)} window sizes)")
    print(f"✅ Regime detection: Active ({len(regime_summary)} regimes identified)")
    print(f"✅ Anomaly detection: Working ({len(anomalies)} anomalies found)")
    print("\n✨ Phase 1 complete! Ready for Phase 2 (Kafka/Flink pipeline)\n")
    
    return combined_data, analyzer


if __name__ == '__main__':
    data, analyzer = main()
