#!/usr/bin/env python3
"""Live CLI Dashboard - with debug output"""

import sys
import time
import json
from datetime import datetime
from collections import deque
import os

sys.path.insert(0, './flink_jobs')
from job_2_correlation import CorrelationCalculator

from kafka import KafkaConsumer

GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BOLD = '\033[1m'
CYAN = '\033[96m'
RESET = '\033[0m'


class DashboardState:
    def __init__(self):
        self.current_prices = {}
        self.current_correlations_1h = {}
        self.current_regime = None
        self.messages_received = 0
        self.last_update = datetime.now()
        self.status = "Connecting..."
        self.buffer_size = 0
        self.debug_info = ""


def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')


def render_dashboard(state: DashboardState):
    clear_screen()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{BOLD}{CYAN}╔════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║{RESET}  {BOLD}Market Regime Detector - Live Dashboard{RESET}")
    print(f"{BOLD}{CYAN}║{RESET}  Time: {now}")
    print(f"{BOLD}{CYAN}╠════════════════════════════════════════════════════════════════╣{RESET}")
    
    # DEBUG
    print(f"\n{YELLOW}DEBUG:{RESET}")
    print(f"  Buffer size: {state.buffer_size}")
    print(f"  {state.debug_info}")
    
    # Prices
    print(f"\n{BOLD}Current Prices:{RESET}")
    if state.current_prices:
        for symbol in ['eur_usd', 'oil', 'gold', 'sp500', 'vix', 'bund_yield']:
            if symbol in state.current_prices:
                price = state.current_prices[symbol]
                print(f"  {symbol:12} {price:>10.2f}")
    
    # Correlations
    print(f"\n{BOLD}1-Hour Window:{RESET}")
    if state.current_regime:
        print(f"  {BOLD}Regime:{RESET} {state.current_regime.upper()}")
    
    if state.current_correlations_1h:
        print(f"  {BOLD}Correlations:{RESET}")
        for symbol in sorted(state.current_correlations_1h.keys()):
            if symbol == 'eur_usd':
                continue
            corr = state.current_correlations_1h[symbol]
            print(f"    {symbol:12} {corr:+.3f}")
    
    # Status
    print(f"\n{BOLD}{CYAN}╠════════════════════════════════════════════════════════════════╣{RESET}")
    print(f"{BOLD}{CYAN}║{RESET}  {GREEN}{state.status}{RESET} | Messages: {state.messages_received:,} | Last: {state.last_update.strftime('%H:%M:%S')}")
    print(f"{BOLD}{CYAN}╚════════════════════════════════════════════════════════════════╝{RESET}")


def read_from_kafka(state: DashboardState):
    try:
        consumer = KafkaConsumer(
            'fx-rates', 'commodities', 'indices', 'volatility', 'yields',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=1000,
            group_id='dashboard-consumer'
        )
        
        state.status = "Connected"
        calc = CorrelationCalculator(window_size_minutes=60)
        
        while True:
            try:
                message = consumer.poll(timeout_ms=1000)
                
                if message:
                    for topic_partition, records in message.items():
                        for record in records:
                            data = record.value
                            timestamp = data.get('timestamp', datetime.utcnow().isoformat())
                            
                            for key in ['data', 'commodities', 'indices', 'volatility', 'yields']:
                                if key in data and isinstance(data[key], dict):
                                    for symbol, value in data[key].items():
                                        if symbol != 'timestamp':
                                            try:
                                                val = float(value)
                                                state.current_prices[symbol] = val
                                                calc.add_data_point(timestamp, symbol, val)
                                            except (ValueError, TypeError):
                                                pass  # Skip non-numeric values
                            
                            state.current_correlations_1h = calc.calculate_correlations()
                            state.current_regime = calc.get_regime()
                            state.messages_received += 1
                            state.last_update = datetime.now()
                            
                            # DEBUG: Check buffer
                            state.buffer_size = len(calc.data_buffer.get('eur_usd', []))
                            state.debug_info = f"EUR/USD points: {state.buffer_size}"
                
                render_dashboard(state)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                state.debug_info = f"Error: {str(e)[:40]}"
                render_dashboard(state)
        
        consumer.close()
    
    except Exception as e:
        print(f"\n{RED}Fatal: {e}{RESET}")


def main():
    state = DashboardState()
    read_from_kafka(state)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Stopped{RESET}")
