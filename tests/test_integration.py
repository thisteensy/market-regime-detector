import pytest
from market_regime_detector.data_fetcher import DataFetcher
import pandas as pd


class TestIntegration:
    def test_full_pipeline(self):
        """Test that data fetcher produces valid data structure"""
        fetcher = DataFetcher()
        data = fetcher.fetch_all()
        
        # Should return a DataFrame
        assert isinstance(data, pd.DataFrame)
        
        # Should have timestamp column
        assert 'timestamp' in data.columns
        
        # If data was fetched, prices should be positive
        if len(data) > 0:
            for col in data.columns:
                if col != 'timestamp':
                    assert (data[col] > 0).all(), f"{col} has non-positive values"
    
    def test_dataframe_structure(self):
        """Test that returned data is a valid DataFrame"""
        fetcher = DataFetcher()
        data = fetcher.fetch_all()
        
        assert isinstance(data, pd.DataFrame)
        assert 'timestamp' in data.columns
