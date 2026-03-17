#!/usr/bin/env python3
"""
Flink Job 2: Rolling Correlation Analysis

Calculates rolling correlations between EUR/USD and other instruments.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List
from collections import defaultdict
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CorrelationCalculator:
    """Calculates Pearson correlation in rolling windows."""
    
    def __init__(self, window_size_minutes=60):
        self.window_size_minutes = window_size_minutes
        self.data_buffer = defaultdict(list)
    
    def add_data_point(self, timestamp: str, symbol: str, value: float):
        """Add data point and clean old data."""
        try:
            ts = datetime.fromisoformat(timestamp)
            self.data_buffer[symbol].append({'timestamp': ts, 'value': float(value)})
            
            # Remove old data outside window
            cutoff_seconds = ts.timestamp() - (self.window_size_minutes * 60)
            for sym in self.data_buffer:
                self.data_buffer[sym] = [
                    p for p in self.data_buffer[sym]
                    if p['timestamp'].timestamp() >= cutoff_seconds
                ]
        except Exception as e:
            logger.error(f"Error adding data: {e}")
    
    def calculate_correlations(self) -> Dict[str, float]:
        """Calculate Pearson correlation with EUR/USD."""
        try:
            if len(self.data_buffer.get('eur_usd', [])) < 2:
                return {}
            
            eur_usd_data = self.data_buffer['eur_usd']
            eur_times = {p['timestamp'].timestamp(): p['value'] for p in eur_usd_data}
            
            correlations = {}
            
            for symbol in self.data_buffer:
                if symbol == 'eur_usd':
                    correlations['eur_usd'] = 1.0
                    continue
                
                inst_data = self.data_buffer[symbol]
                inst_times = {p['timestamp'].timestamp(): p['value'] for p in inst_data}
                
                common_times = set(eur_times.keys()) & set(inst_times.keys())
                if len(common_times) < 2:
                    correlations[symbol] = 0.0
                    continue
                
                eur_vals = [eur_times[t] for t in sorted(common_times)]
                inst_vals = [inst_times[t] for t in sorted(common_times)]
                
                corr = self._pearson(eur_vals, inst_vals)
                correlations[symbol] = round(corr, 3)
            
            return correlations
        except Exception as e:
            logger.error(f"Error calculating: {e}")
            return {}
    
    @staticmethod
    def _pearson(x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        if len(x) < 2 or len(x) != len(y):
            return 0.0
        
        n = len(x)
        x_mean = sum(x) / n
        y_mean = sum(y) / n
        
        cov = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n)) / n
        x_var = sum((xi - x_mean) ** 2 for xi in x) / n
        y_var = sum((yi - y_mean) ** 2 for yi in y) / n
        
        x_std = math.sqrt(x_var)
        y_std = math.sqrt(y_var)
        
        if x_std == 0 or y_std == 0:
            return 0.0
        
        return cov / (x_std * y_std)
    
    def get_regime(self) -> str:
        """Which instrument is EUR/USD following?"""
        corrs = self.calculate_correlations()
        if not corrs:
            return None
        other = {k: abs(v) for k, v in corrs.items() if k != 'eur_usd'}
        if not other:
            return None
        return max(other.items(), key=lambda x: x[1])[0]


if __name__ == '__main__':
    print("Job 2: Correlation Analysis - calculates rolling correlations")
