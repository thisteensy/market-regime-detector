#!/usr/bin/env python3
"""Kafka Data Producer - Stream market data"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional
import os
from dotenv import load_dotenv
from data_fetcher import DataFetcher

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaDataProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.message_count = 0
        
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
    
    def publish(self, topic: str, message: Dict) -> bool:
        try:
            if self.producer:
                self.producer.send(topic, value=message)
                self.message_count += 1
                return True
        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}")
        return False
    
    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer closed. Published {self.message_count} messages total.")


class DataStreamingService:
    def __init__(self, producer: KafkaDataProducer, fetch_interval_seconds: int = 600):
        self.producer = producer
        self.fetch_interval = fetch_interval_seconds
        self.fetcher = DataFetcher()
        self.running = False
    
    def run_once(self) -> None:
        try:
            data = self.fetcher.fetch_all()
            if len(data) > 0:
                row = data.iloc[-1].copy()
                timestamp = datetime.now(timezone.utc).isoformat()
                
                # Publish only available columns
                message = {
                    'timestamp': timestamp,
                    'data': {}
                }
                
                for col in row.index:
                    if col != 'timestamp':
                        try:
                            message['data'][col] = float(row[col])
                        except:
                            pass
                
                self.producer.publish('market-data', message)
                logger.info(f"Published {len(message['data'])} instruments")
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
    
    def start_streaming(self, duration_seconds: Optional[int] = None) -> None:
        self.running = True
        start_time = time.time()
        
        logger.info(f"🚀 Starting data stream (interval: {self.fetch_interval}s)")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while self.running:
                elapsed = time.time() - start_time
                
                if duration_seconds and elapsed > duration_seconds:
                    logger.info(f"Duration limit reached ({duration_seconds}s)")
                    break
                
                self.run_once()
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping stream (Ctrl+C)")
        finally:
            self.producer.close()
            logger.info("Stream stopped")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream market data to Kafka')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds')
    parser.add_argument('--interval', type=int, default=600, help='Fetch interval in seconds')
    parser.add_argument('--kafka', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print(" EUR/USD CORRELATION PIPELINE - KAFKA DATA PRODUCER")
    print("="*70)
    print(f"Kafka: {args.kafka}")
    print()
    
    producer = KafkaDataProducer(bootstrap_servers=args.kafka)
    service = DataStreamingService(producer=producer, fetch_interval_seconds=args.interval)
    service.start_streaming(duration_seconds=args.duration)


if __name__ == '__main__':
    main()
