"""
Data Source Integration Layer
Fetches real market data from free APIs
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSourceConfig:
    """Configuration for free data sources"""
    
    # Yahoo Finance (free, no API key needed)
    YAHOO_BASE = "https://query1.finance.yahoo.com"
    
    # Alpha Vantage (free tier, limited calls)
    ALPHA_VANTAGE_BASE = "https://www.alphavantage.co"
    ALPHA_VANTAGE_KEY = "demo"  # Replace with your key for higher limits
    
    # ECB rates (free, official)
    ECB_BASE = "https://www.ecb.europa.eu/stats/eurofxref"


class DataFetcher:
    """Orchestrates fetching data from multiple sources"""
    
    def __init__(self, lookback_days: int = 90):
        """
        Args:
            lookback_days: How far back to fetch historical data
        """
        self.lookback_days = lookback_days
        self.lookback_start = datetime.now() - timedelta(days=lookback_days)
        self.data = {}
    
    def fetch_all(self) -> pd.DataFrame:
        """Fetch all instruments and combine into single dataframe"""
        print("\n" + "="*60)
        print("🌐 Fetching Market Data")
        print("="*60)
        
        # Fetch each instrument
        print("\n📊 EUR/USD Exchange Rate...")
        self.data['eur_usd'] = self._fetch_eurusd()
        
        print("⛽ Oil (WTI Crude)...")
        self.data['oil'] = self._fetch_oil()
        
        print("🏆 Gold...")
        self.data['gold'] = self._fetch_gold()
        
        print("📈 S&P 500...")
        self.data['sp500'] = self._fetch_sp500()
        
        print("📊 VIX (Volatility Index)...")
        self.data['vix'] = self._fetch_vix()
        
        print("📉 German Bund 10Y Yield...")
        self.data['bund_yield'] = self._fetch_bund_yield()
        
        # Combine all into single dataframe
        combined = self._combine_datasets()
        print(f"\n✅ Combined {len(combined)} data points across all instruments")
        
        return combined
    
    def _fetch_eurusd(self) -> pd.DataFrame:
        """Fetch EUR/USD from Yahoo Finance"""
        try:
            # Yahoo Finance symbol for EUR/USD
            url = f"{DataSourceConfig.YAHOO_BASE}/v10/finance/quoteSummary/EURUSD=X"
            params = {
                'modules': 'price',
                'region': 'US',
                'lang': 'en'
            }
            
            # Alternative: use yfinance library if available
            try:
                import yfinance as yf
                data = yf.download('EURUSD=X', 
                                  start=self.lookback_start.date(),
                                  progress=False)
                df = pd.DataFrame({
                    'timestamp': data.index,
                    'eur_usd': data['Close'].values
                })
                print(f"  ✓ Fetched {len(df)} EUR/USD quotes")
                return df.reset_index(drop=True)
            except ImportError:
                print("  ⚠ yfinance not available, using mock data (install: pip install yfinance)")
                return self._mock_eurusd()
                
        except Exception as e:
            logger.error(f"Error fetching EUR/USD: {e}")
            return self._mock_eurusd()
    
    def _fetch_oil(self) -> pd.DataFrame:
        """Fetch WTI Crude Oil prices"""
        try:
            import yfinance as yf
            data = yf.download('CL=F',  # WTI Crude Oil futures
                              start=self.lookback_start.date(),
                              progress=False)
            df = pd.DataFrame({
                'timestamp': data.index,
                'oil': data['Close'].values
            })
            print(f"  ✓ Fetched {len(df)} oil quotes")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching oil: {e}")
            return self._mock_oil()
    
    def _fetch_gold(self) -> pd.DataFrame:
        """Fetch Gold prices"""
        try:
            import yfinance as yf
            data = yf.download('GC=F',  # Gold futures
                              start=self.lookback_start.date(),
                              progress=False)
            df = pd.DataFrame({
                'timestamp': data.index,
                'gold': data['Close'].values
            })
            print(f"  ✓ Fetched {len(df)} gold quotes")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching gold: {e}")
            return self._mock_gold()
    
    def _fetch_sp500(self) -> pd.DataFrame:
        """Fetch S&P 500 index"""
        try:
            import yfinance as yf
            data = yf.download('^GSPC',  # S&P 500
                              start=self.lookback_start.date(),
                              progress=False)
            df = pd.DataFrame({
                'timestamp': data.index,
                'sp500': data['Close'].values
            })
            print(f"  ✓ Fetched {len(df)} S&P 500 quotes")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching S&P 500: {e}")
            return self._mock_sp500()
    
    def _fetch_vix(self) -> pd.DataFrame:
        """Fetch VIX volatility index"""
        try:
            import yfinance as yf
            data = yf.download('^VIX',  # VIX
                              start=self.lookback_start.date(),
                              progress=False)
            df = pd.DataFrame({
                'timestamp': data.index,
                'vix': data['Close'].values
            })
            print(f"  ✓ Fetched {len(df)} VIX quotes")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching VIX: {e}")
            return self._mock_vix()
    
    def _fetch_bund_yield(self) -> pd.DataFrame:
        """Fetch German Bund 10Y yield"""
        try:
            import yfinance as yf
            # German Bund 10Y yield
            data = yf.download('DBXD',  # iShares Germany Bund UCITS ETF
                              start=self.lookback_start.date(),
                              progress=False)
            
            # Convert ETF price to approximate yield (rough proxy)
            # In real implementation, fetch from ECB directly
            df = pd.DataFrame({
                'timestamp': data.index,
                'bund_yield': 2.5 + (data['Close'].values - data['Close'].iloc[0]) * 0.01
            })
            print(f"  ✓ Fetched {len(df)} Bund yield estimates")
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error fetching Bund yield: {e}")
            return self._mock_bund_yield()
    
    # Mock data generators for fallback
    def _mock_eurusd(self) -> pd.DataFrame:
        """Generate realistic synthetic EUR/USD data"""
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.002)
        return pd.DataFrame({
            'timestamp': dates,
            'eur_usd': 1.0850 + base * 0.01 + np.random.randn(len(dates)) * 0.003
        })
    
    def _mock_oil(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.3)
        return pd.DataFrame({
            'timestamp': dates,
            'oil': 75.0 + base + np.random.randn(len(dates)) * 1.0
        })
    
    def _mock_gold(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 1.0)
        return pd.DataFrame({
            'timestamp': dates,
            'gold': 2050.0 + base * 10 + np.random.randn(len(dates)) * 5.0
        })
    
    def _mock_sp500(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 3.0)
        return pd.DataFrame({
            'timestamp': dates,
            'sp500': 5000.0 + base * 10 + np.random.randn(len(dates)) * 20.0
        })
    
    def _mock_vix(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.3)
        return pd.DataFrame({
            'timestamp': dates,
            'vix': 15.0 + np.abs(base) * 0.5 + np.random.randn(len(dates)) * 0.5
        })
    
    def _mock_bund_yield(self) -> pd.DataFrame:
        dates = pd.date_range(start=self.lookback_start, periods=1440, freq='1h')
        base = np.cumsum(np.random.randn(len(dates)) * 0.01)
        return pd.DataFrame({
            'timestamp': dates,
            'bund_yield': 2.5 + base * 0.01 + np.random.randn(len(dates)) * 0.02
        })
    
    def _combine_datasets(self) -> pd.DataFrame:
        """Merge all datasets on timestamp (inner join to align)"""
        df = self.data['eur_usd'].copy()
        
        for key, data in self.data.items():
            if key != 'eur_usd':
                # Merge on nearest timestamp to handle slight time misalignments
                df = pd.merge_asof(
                    df.sort_values('timestamp'),
                    data.sort_values('timestamp'),
                    on='timestamp',
                    direction='nearest',
                    tolerance=pd.Timedelta('1h')
                )
        
        # Remove rows with any NaN values
        df = df.dropna()
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df


if __name__ == '__main__':
    # Quick test
    fetcher = DataFetcher(lookback_days=30)
    data = fetcher.fetch_all()
    
    print("\n" + "="*60)
    print("Sample Data (first 5 rows):")
    print("="*60)
    print(data.head())
    
    print("\n" + "="*60)
    print("Data Summary:")
    print("="*60)
    print(data.describe())
