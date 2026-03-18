#!/usr/bin/env python3
"""
Kafka Data Producer
Continuously fetches market data and publishes to Kafka topics.
This will feed data into the Flink streaming jobs.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
import threading
import os
from dotenv import load_dotenv
import pandas as pd
from data_fetcher import DataFetcher

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import Kafka - graceful fallback if not available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    logger.warning("kafka-python not installed. Will simulate Kafka publishing.")
    KAFKA_AVAILABLE = False


class KafkaDataProducer:
    """
    Produces market data to Kafka topics in real-time.
    
    Topics:
    - fx-rates: Exchange rate updates
    - commodities: Oil, Gold prices
    - indices: S&P 500, DAX, indices
    - yields: Bond yields
    - volatility: VIX, VSTOXX
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092', simulation_mode: bool = False):
        """
        Args:
            bootstrap_servers: Kafka broker address
            simulation_mode: If True, simulates Kafka without actual connection
        """
        self.bootstrap_servers = bootstrap_servers
        self.simulation_mode = simulation_mode or not KAFKA_AVAILABLE
        self.producer = None
        self.message_count = 0
        self.last_prices = {}
        
        if not self.simulation_mode:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    request_timeout_ms=30000
                )
                logger.info(f"✓ Connected to Kafka at {bootstrap_servers}")
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}")
                logger.info("Switching to simulation mode")
                self.simulation_mode = True
        else:
            logger.info("📊 Running in simulation mode (no Kafka broker)")
    
    def publish(self, topic: str, message: Dict) -> bool:
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            message: Message dict (will be JSON serialized)
        
        Returns:
            True if successful
        """
        try:
            if self.simulation_mode:
                # In simulation mode, just log the message
                logger.info(f"[{topic}] {message}")
                self.message_count += 1
                return True
            else:
                # Real Kafka
                future = self.producer.send(topic, value=message)
                record_metadata = future.get(timeout=10)
                self.message_count += 1
                return True
        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}")
            return False
    
    def produce_fx_rates(self, rates: Dict[str, float]) -> None:
        """Publish exchange rate update"""
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'yahoo_finance',
            'data': rates,
            'schema_version': '1.0'
        }
        self.publish('fx-rates', message)
    
    def produce_commodities(self, prices: Dict[str, float]) -> None:
        """Publish commodity prices"""
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'yahoo_finance',
            'commodities': prices,
            'schema_version': '1.0'
        }
        self.publish('commodities', message)
    
    def produce_indices(self, indices: Dict[str, float]) -> None:
        """Publish stock index updates"""
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'yahoo_finance',
            'indices': indices,
            'schema_version': '1.0'
        }
        self.publish('indices', message)
    
    def produce_yields(self, yields: Dict[str, float]) -> None:
        """Publish bond yield updates"""
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'ecb',
            'yields': yields,
            'schema_version': '1.0'
        }
        self.publish('yields', message)
    
    def produce_volatility(self, vol: Dict[str, float]) -> None:
        """Publish volatility indices"""
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': 'yahoo_finance',
            'volatility': vol,
            'schema_version': '1.0'
        }
        self.publish('volatility', message)
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer and not self.simulation_mode:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer closed. Published {self.message_count} messages total.")


class DataStreamingService:
    """
    Continuous data streaming service.
    Fetches fresh data periodically and publishes to Kafka.
    """
    
    def __init__(self, 
                 producer: KafkaDataProducer,
                 fetch_interval_seconds: int = 300,
                 backtest_data: Optional[pd.DataFrame] = None):
        """
        Args:
            producer: KafkaDataProducer instance
            fetch_interval_seconds: How often to fetch fresh data (5 min = realistic for free APIs)
            backtest_data: Optional historical data for backtesting/simulation
        """
        self.producer = producer
        self.fetch_interval = fetch_interval_seconds
        self.backtest_data = backtest_data
        self.backtest_index = 0
        self.running = False
        self.fetcher = DataFetcher(lookback_days=90) if backtest_data is None else None
    
    def run_once(self) -> None:
        """Execute a single fetch-and-publish cycle"""
        try:
            if self.backtest_data is not None:
                # Backtesting mode: replay historical data
                if self.backtest_index < len(self.backtest_data):
                    row = self.backtest_data.iloc[self.backtest_index]
                    self._publish_row(row)
                    self.backtest_index += 1
                else:
                    logger.info("Backtest complete!")
                    self.running = False
            else:
                # Live mode: fetch fresh data
                data = self.fetcher.fetch_all()
                if len(data) > 0:
                    row = data.iloc[-1].copy()
                    # Override timestamp to current time
                    row['timestamp'] = datetime.now(timezone.utc).isoformat()
                    self._publish_row(row)
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
    
    def _publish_row(self, row) -> None:
        """Publish a single data row across all topics"""
        timestamp = row['timestamp']
        
        # FX Rates
        self.producer.produce_fx_rates({
            'eur_usd': float(row['eur_usd']),
            'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
        })
        
        # Commodities
        self.producer.produce_commodities({
            'oil': float(row['oil']),
            'gold': float(row['gold']),
            'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
        })
        
        # Indices
        self.producer.produce_indices({
            'sp500': float(row['sp500']),
            'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
        })
        
        # Volatility
        self.producer.produce_volatility({
            'vix': float(row['vix']),
            'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
        })
        
        # Yields
        self.producer.produce_yields({
            'bund_yield': float(row['bund_yield']),
            'timestamp': timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
        })
    
    def start_streaming(self, duration_seconds: Optional[int] = None) -> None:
        """
        Start the streaming service.
        
        Args:
            duration_seconds: How long to stream (None = forever until stopped)
        """
        self.running = True
        start_time = time.time()
        
        logger.info(f"🚀 Starting data stream (interval: {self.fetch_interval}s)")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while self.running:
                elapsed = time.time() - start_time
                
                # Check if we should stop
                if duration_seconds and elapsed > duration_seconds:
                    logger.info(f"Duration limit reached ({duration_seconds}s)")
                    break
                
                # Fetch and publish
                self.run_once()
                
                # Wait before next fetch
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping stream (Ctrl+C)")
        finally:
            self.producer.close()
            logger.info("Stream stopped")
    
    def start_streaming_threaded(self) -> threading.Thread:
        """Start streaming in a background thread"""
        thread = threading.Thread(target=self.start_streaming, daemon=True)
        thread.start()
        return thread


def main():
    """
    Main entry point: demonstration of data producer
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream market data to Kafka')
    
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration in seconds (backtest mode)')
    parser.add_argument('--interval', type=int, default=5,
                        help='Fetch interval in seconds')
    parser.add_argument('--kafka', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--lookback-days', type=int, default=30,
                        help='Days of historical data to load (backtest mode)')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print(" EUR/USD CORRELATION PIPELINE - KAFKA DATA PRODUCER")
    print("="*70)
    print(f"Kafka: {args.kafka}")
    print()
    
    # Create producer
    producer = KafkaDataProducer(
        bootstrap_servers=args.kafka,
        simulation_mode=False  # Simulation for demo
    )
    
    # Create streaming service
    if True:
        # Load historical data for backtesting
        logger.info("Loading historical data for playback...")
        fetcher = DataFetcher(lookback_days=args.lookback_days)
        backtest_data = fetcher.fetch_all()
        
        service = DataStreamingService(
            producer=producer,
            fetch_interval_seconds=args.interval,
            backtest_data=backtest_data
        )
        
        logger.info(f"Loaded {len(backtest_data)} data points for playback")
    else:
        # Live streaming
        service = DataStreamingService(
            producer=producer,
            fetch_interval_seconds=args.interval
        )
    
    # Start streaming
    service.start_streaming(duration_seconds=args.duration)


if __name__ == '__main__':
    main()
