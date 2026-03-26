import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from market_regime_detector.api import app


class TestAPI:
    def setup_method(self):
        """Set up test client"""
        self.app = app
        self.client = self.app.test_client()
    
    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = self.client.get('/api/health')
        assert response.status_code == 200
        data = response.get_json()
        assert 'status' in data
    
    def test_correlations_endpoint(self):
        """Test correlations endpoint returns JSON"""
        response = self.client.get('/api/correlations')
        assert response.status_code == 200
        data = response.get_json()
        assert 'correlations' in data
        assert 'regime' in data
    
    def test_correlations_window_parameter(self):
        """Test window parameter is respected"""
        response = self.client.get('/api/correlations?window=360')
        assert response.status_code == 200
        data = response.get_json()
        assert data['window_minutes'] == 360
