"""
EUR/USD Streaming Correlation Analysis
Phase 1: Local validation with pandas
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Dict, List, Tuple
import warnings

warnings.filterwarnings('ignore')


class CorrelationAnalyzer:
    """
    Calculates rolling correlations between EUR/USD and multiple market drivers.
    Designed to mirror the logic that will run in Flink.
    """

    def __init__(self, window_hours: List[int] = None, slide_hours: int = 1):
        """
        Args:
            window_hours: List of window sizes (e.g., [1, 4, 24] for 1h, 4h, daily)
            slide_hours: How far to slide the window each calculation (overlap)
        """
        self.window_hours = window_hours or [1, 4, 24]
        self.slide_hours = slide_hours
        self.data = None
        self.normalized_data = None
        self.correlations = {}

    def load_data(self, df: pd.DataFrame) -> None:
        """
        Load combined dataframe with all instruments.
        Expected columns: timestamp, eur_usd, oil, gold, sp500, vix, bund_yield
        """
        self.data = df.copy()
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.data = self.data.sort_values('timestamp').reset_index(drop=True)
        print(f"✓ Loaded {len(self.data)} rows")
        print(f"  Date range: {self.data['timestamp'].min()} to {self.data['timestamp'].max()}")

    def normalize(self) -> pd.DataFrame:
        """
        Normalize each instrument to Z-scores (mean=0, std=1).
        This allows correlation to be comparable across instruments with different scales.
        """
        self.normalized_data = self.data.copy()
        
        for col in ['eur_usd', 'oil', 'gold', 'sp500', 'vix', 'bund_yield']:
            if col in self.normalized_data.columns:
                mean = self.normalized_data[col].mean()
                std = self.normalized_data[col].std()
                if std > 0:
                    self.normalized_data[col] = (self.normalized_data[col] - mean) / std
                else:
                    self.normalized_data[col] = 0
        
        print("✓ Data normalized to Z-scores")
        return self.normalized_data

    def calculate_correlations(self) -> Dict:
        """
        Calculate Pearson correlation coefficients for each window size.
        Returns rolling correlations of each instrument vs. EUR/USD.
        """
        if self.normalized_data is None:
            self.normalize()

        results = {}

        for window_h in self.window_hours:
            window_samples = window_h * 60  # Assume 1 sample per minute
            correlations_list = []

            # Slide across the data
            for i in range(0, len(self.normalized_data) - window_samples, self.slide_hours * 60):
                window_end = min(i + window_samples, len(self.normalized_data))
                window_data = self.normalized_data.iloc[i:window_end]

                timestamp = window_data['timestamp'].iloc[-1]

                # Calculate correlations with EUR/USD
                eur_usd_prices = window_data['eur_usd']
                corr_dict = {'timestamp': timestamp}

                for instrument in ['oil', 'gold', 'sp500', 'vix', 'bund_yield']:
                    if instrument in window_data.columns:
                        corr = eur_usd_prices.corr(window_data[instrument])
                        # Handle NaN (can happen with constant series)
                        corr_dict[instrument] = round(corr, 3) if not np.isnan(corr) else 0.0

                correlations_list.append(corr_dict)

            results[f'{window_h}h'] = pd.DataFrame(correlations_list)
            print(f"✓ Calculated {len(correlations_list)} correlation windows for {window_h}h")

        self.correlations = results
        return results

    def detect_regime_change(self, window_h: int = 4, threshold: float = 0.3) -> pd.DataFrame:
        """
        Detect when correlations change significantly (regime shift).
        A regime is defined as which instrument EUR/USD is "following" most closely.
        """
        if f'{window_h}h' not in self.correlations:
            raise ValueError(f"No correlations calculated for {window_h}h window")

        corr_df = self.correlations[f'{window_h}h'].copy()
        instruments = ['oil', 'gold', 'sp500', 'vix', 'bund_yield']

        # Find strongest correlation for each window
        def get_regime(row):
            abs_corrs = {instr: abs(row[instr]) for instr in instruments if instr in row}
            if not abs_corrs:
                return None, 0.0
            strongest = max(abs_corrs.items(), key=lambda x: x[1])
            return strongest[0], row[strongest[0]]

        corr_df[['regime', 'regime_strength']] = corr_df.apply(
            lambda row: pd.Series(get_regime(row)), axis=1
        )

        # Detect shifts
        corr_df['regime_change'] = corr_df['regime'] != corr_df['regime'].shift(1)

        print(f"✓ Detected {corr_df['regime_change'].sum()} regime changes in {window_h}h window")
        return corr_df

    def detect_anomalies(self, window_h: int = 1, zscore_threshold: float = 2.0) -> pd.DataFrame:
        """
        Detect price spikes that don't correlate with known drivers.
        A spike is "unexplained" if the price moved >2σ but correlations are weak.
        """
        if self.data is None:
            raise ValueError("No data loaded")

        prices = self.data.copy()
        instruments = ['oil', 'gold', 'sp500', 'vix', 'bund_yield']

        # Calculate price change (returns)
        prices['eur_usd_change'] = prices['eur_usd'].pct_change() * 100  # In basis points

        # Z-score of returns
        mean_change = prices['eur_usd_change'].mean()
        std_change = prices['eur_usd_change'].std()
        prices['eur_usd_zscore'] = (prices['eur_usd_change'] - mean_change) / std_change

        # Find spikes
        prices['is_spike'] = abs(prices['eur_usd_zscore']) > zscore_threshold

        # For spikes, check if they're "explained" by correlated moves
        anomalies = prices[prices['is_spike']].copy()

        if len(anomalies) > 0:
            print(f"✓ Detected {len(anomalies)} potential spikes (>{zscore_threshold}σ moves)")
        
        return anomalies

    def export_to_json(self, output_dir: str = './correlation_output') -> None:
        """Export results for downstream processing (Flink simulation)"""
        Path(output_dir).mkdir(exist_ok=True)

        for window_h, corr_df in self.correlations.items():
            output_file = f"{output_dir}/correlations_{window_h}.json"
            # Convert to records format for easier consumption
            records = corr_df.to_dict(orient='records')
            with open(output_file, 'w') as f:
                json.dump(records, f, indent=2, default=str)
            print(f"✓ Exported {len(records)} records to {output_file}")


def main():
    """
    Example usage: Load sample data and validate correlation math
    """
    print("\n" + "="*60)
    print("EUR/USD Correlation Analysis - Local Validation")
    print("="*60 + "\n")

    analyzer = CorrelationAnalyzer(window_hours=[1, 4, 24], slide_hours=1)

    # For now, we'll create synthetic data to demonstrate the pipeline works
    # In the next step, we'll replace this with real API data
    print("📊 Generating sample data (will be replaced with real API data)...\n")
    
    # Create synthetic correlated data
    np.random.seed(42)
    dates = pd.date_range(start='2025-02-01', periods=2880, freq='30min')  # 60 days of 30-min data
    
    base_signal = np.cumsum(np.random.randn(len(dates)) * 0.01)
    
    data = pd.DataFrame({
        'timestamp': dates,
        'eur_usd': 1.08 + base_signal * 0.02 + np.random.randn(len(dates)) * 0.005,
        'oil': 75 + base_signal * 5 + np.random.randn(len(dates)) * 1,
        'gold': 2050 + base_signal * 20 + np.random.randn(len(dates)) * 5,
        'sp500': 5000 + base_signal * 50 + np.random.randn(len(dates)) * 10,
        'vix': 15 + np.abs(base_signal) * 3 + np.random.randn(len(dates)) * 0.5,
        'bund_yield': 2.5 + base_signal * 0.1 + np.random.randn(len(dates)) * 0.05,
    })

    # Load and analyze
    analyzer.load_data(data)
    analyzer.normalize()
    analyzer.calculate_correlations()
    
    # Regime detection
    print("\n📈 Regime Analysis (4-hour window):")
    regime_df = analyzer.detect_regime_change(window_h=4)
    print(regime_df[['timestamp', 'regime', 'regime_strength']].tail(10))
    
    # Anomaly detection
    print("\n🚨 Anomaly Detection:")
    anomalies = analyzer.detect_anomalies(window_h=1, zscore_threshold=2.0)
    if len(anomalies) > 0:
        print(anomalies[['timestamp', 'eur_usd_change', 'eur_usd_zscore']].head())
    
    # Export for next phase
    analyzer.export_to_json()
    
    print("\n✅ Phase 1 validation complete!")
    print("Next: Integrate real API data sources\n")


if __name__ == '__main__':
    main()
