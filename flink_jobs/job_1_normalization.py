"""
Flink Job 1: Normalization & Enrichment

Reads raw market data from Kafka, normalizes to common schema.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NormalizationMapper:
    """Converts raw market data to normalized schema."""
    
    def map(self, raw_message: str):
        """Convert raw data to normalized format."""
        try:
            data = json.loads(raw_message)
            timestamp = data.get('timestamp', datetime.utcnow().isoformat())
            source = data.get('source', 'unknown')
            
            normalized_records = []
            
            # Extract all instrument types
            for data_type, instruments in [
                ('fx_rate', data.get('data', {})),
                ('commodity', data.get('commodities', {})),
                ('index', data.get('indices', {})),
                ('volatility', data.get('volatility', {})),
                ('yield', data.get('yields', {}))
            ]:
                for symbol, value in instruments.items():
                    try:
                        normalized = {
                            'timestamp': timestamp,
                            'symbol': symbol,
                            'value': float(value),
                            'source': source,
                            'confidence': 0.95,
                            'metadata': {'data_type': data_type}
                        }
                        normalized_records.append(normalized)
                    except (ValueError, TypeError):
                        pass
            
            for record in normalized_records:
                yield json.dumps(record)
        
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON")
        except Exception as e:
            logger.error(f"Error: {e}")


if __name__ == '__main__':
    print("Job 1: Normalization - converts raw data to standard schema")