"""
Global Market Data Fetcher - 24/5 Coverage
Only includes successfully fetched data, no fallbacks
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import pytz

script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataSourceConfig:
    TWELVEDATA_BASE = "https://api.twelvedata.com"
    TWELVEDATA_KEY = os.getenv("TWELVEDATA_API_KEY", "")


class ExchangeHours:
    @staticmethod
    def is_us_hours():
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        if now.weekday() >= 5:  # Weekend
            return False
        # 9:30 AM - 4:00 PM ET
        time_in_minutes = now.hour * 60 + now.minute
        return (9 * 60 + 30) <= time_in_minutes <= (16 * 60)
    
    @staticmethod
    def is_asian_hours():
        jst = pytz.timezone('Asia/Tokyo')
        now = datetime.now(jst)
        if now.weekday() >= 5:  # Weekend
            return False
        # 9:00 AM - 3:00 PM JST
        return 9 <= now.hour <= 15


class DataFetcher:
    def __init__(self):
        self.twelvedata_key = DataSourceConfig.TWELVEDATA_KEY
        
        self.symbols = {
            'eur_usd': 'EUR/USD',
            'oil': 'USO',
            'gold': 'GLD',
            'sp500': 'SPY',
            'vix': 'UVXY',
            'bund': 'IGOV',
            'nikkei': 'EWJ'
        }
        
        self.us_hours = ExchangeHours.is_us_hours()
        self.asian_hours = ExchangeHours.is_asian_hours()
    
    def fetch_all(self) -> pd.DataFrame:
        print("\n" + "="*60)
        print("🌐 Global Market Data (Real data only)")
        print("="*60)
        
        if not self.twelvedata_key:
            print("❌ No API key!")
            return pd.DataFrame()
        
        values = {'timestamp': [datetime.now()]}
        
        # 24/5
        print("\n📊 EUR/USD (24/5)...")
        price = self._get_price('eur_usd')
        if price:
            values['eur_usd'] = [price]
        
        print("⛽ Oil - USO (24/5)...")
        price = self._get_price('oil')
        if price:
            values['oil'] = [price]
        
        print("🏆 Gold - GLD (24/5)...")
        price = self._get_price('gold')
        if price:
            values['gold'] = [price]
        
        # US Hours
        if self.us_hours:
            print("📈 S&P 500 - SPY (US hours)...")
            price = self._get_price('sp500')
            if price:
                values['sp500'] = [price]
            
            print("📊 Volatility - UVXY (US hours)...")
            price = self._get_price('vix')
            if price:
                values['vix'] = [price]
            
            print("📉 Bonds - IGOV (US hours)...")
            price = self._get_price('bund')
            if price:
                values['bund'] = [price]
        else:
            print("⏰ US markets closed")
        
        # Asian Hours
        if self.asian_hours:
            print("🗾 Nikkei - EWJ (Asian hours)...")
            price = self._get_price('nikkei')
            if price:
                values['nikkei'] = [price]
        else:
            print("⏰ Asian markets closed")
        
        result = pd.DataFrame(values)
        print(f"\n✅ Fetched {len(values)-1} instruments")
        return result
    
    def _get_price(self, key: str) -> float:
        """Fetch price or return None"""
        try:
            symbol = self.symbols[key]
            url = f"{DataSourceConfig.TWELVEDATA_BASE}/quote"
            params = {'symbol': symbol, 'apikey': self.twelvedata_key}
            
            response = requests.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                price = float(data.get('close', 0))
                
                if price > 0:
                    print(f"  ✓ {key}: {price}")
                    return price
            
            print(f"  ✗ {key}: Failed")
            return None
            
        except Exception as e:
            print(f"  ✗ {key}: {e}")
            return None


if __name__ == '__main__':
    fetcher = DataFetcher()
    data = fetcher.fetch_all()
    print("\nData:")
    print(data)
