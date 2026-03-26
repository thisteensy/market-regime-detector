import pytest
from market_regime_detector.data_fetcher import DataFetcher, ExchangeHours
from datetime import datetime


class TestDataFetcher:
    def test_fetcher_initializes(self):
        """Test that DataFetcher can be instantiated"""
        fetcher = DataFetcher()
        assert fetcher is not None
    
    def test_fetch_all_returns_dataframe(self):
        """Test that fetch_all returns a DataFrame"""
        fetcher = DataFetcher()
        data = fetcher.fetch_all()
        assert data is not None
        assert len(data) > 0
    
    def test_fetch_has_required_columns(self):
        """Test that fetched data has EUR/USD, oil, gold at minimum"""
        fetcher = DataFetcher()
        data = fetcher.fetch_all()
        assert 'timestamp' in data.columns
        assert 'eur_usd' in data.columns
        assert 'oil' in data.columns
        assert 'gold' in data.columns
    
    def test_prices_are_positive(self):
        """Test that prices are positive values"""
        fetcher = DataFetcher()
        data = fetcher.fetch_all()
        
        for col in ['eur_usd', 'oil', 'gold']:
            if col in data.columns:
                assert (data[col] > 0).all(), f"{col} should be positive"


class TestExchangeHours:
    def test_is_us_hours_returns_bool(self):
        """Test that is_us_hours returns a boolean"""
        result = ExchangeHours.is_us_hours()
        assert isinstance(result, bool)
