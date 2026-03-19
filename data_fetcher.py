"""
Data Source Integration Layer
Generates synthetic market data for streaming pipeline
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFetcher:
    """Generates realistic synthetic market data"""
    
    def __init__(self, lookback_days: int = 90):
        self.lookback_days = lookback_days
        self.lookback_start = datetime.now() - timedelta(days=lookback_days)
        self.data = {}
    
    def fetch_all(self) -> pd.DataFrame:
        """Generate all instruments and combine into single dataframe"""
        print("\n" + "="*60)
        print("🌐 Fetching Market Data")
        print("="*60)
        
        print("\n📊 EUR/USD Exchange Rate...")
        self.data['eur_usd'] = self._generate_eurusd()
        
        print("⛽ Oil (WTI Crude)...")
        self.data['oil'] = self._generate_oil()
        
        print("🏆 Gold...")
        self.data['gold'] = self._generate_gold()
        
        print("📈 S&P 500...")
        self.data['sp500'] = self._generate_sp500()
        
        print("📊 VIX (Volatility Index)...")
        self.data['vix'] = self._generate_vix()
        
        print("📉 German Bund 10Y Yield...")
        self.data['bund_yield'] = self._generate_bund_yield()
        
        combined = self._combine_datasets()
        print(f"\n✅ Combined {len(combined)} data points across all instruments")
        
        return combined
    
    def _generate_eurusd(self) -> pd.DataFrame:
        """Generate realistic EUR/USD data"""
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.002)
        return pd.DataFrame({
            'timestamp': dates,
            'eur_usd': 1.0850 + base * 0.01 + np.random.randn(len(dates)) * 0.003
        })
    
    def _generate_oil(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.3)
        return pd.DataFrame({
            'timestamp': dates,
            'oil': 75.0 + base + np.random.randn(len(dates)) * 1.0
        })
    
    def _generate_gold(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 1.0)
        return pd.DataFrame({
            'timestamp': dates,
            'gold': 2050.0 + base * 10 + np.random.randn(len(dates)) * 5.0
        })
    
    def _generate_sp500(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 3.0)
        return pd.DataFrame({
            'timestamp': dates,
            'sp500': 5000.0 + base * 10 + np.random.randn(len(dates)) * 20.0
        })
    
    def _generate_vix(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.3)
        return pd.DataFrame({
            'timestamp': dates,
            'vix': 15.0 + np.abs(base) * 0.5 + np.random.randn(len(dates)) * 0.5
        })
    
    def _generate_bund_yield(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.01)
        return pd.DataFrame({
            'timestamp': dates,
            'bund_yield': 2.5 + base * 0.01 + np.random.randn(len(dates)) * 0.02
        })
    
    def _combine_datasets(self) -> pd.DataFrame:
        """Merge all datasets on timestamp"""
        df = self.data['eur_usd'].copy()
        
        for key, data in self.data.items():
            if key != 'eur_usd':
                df = pd.merge_asof(
                    df.sort_values('timestamp'),
                    data.sort_values('timestamp'),
                    on='timestamp',
                    direction='nearest',
                    tolerance=pd.Timedelta('1h')
                )
        
        df = df.dropna()
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df


if __name__ == '__main__':
    fetcher = DataFetcher(lookback_days=30)
    data = fetcher.fetch_all()
    print("\nSample Data (first 5 rows):")
    print(data.head())
