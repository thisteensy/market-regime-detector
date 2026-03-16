# EUR/USD Correlation Pipeline - Setup & Deployment Guide

## Overview

This is a **real-time streaming analysis system** that correlates EUR/USD exchange rates with commodities, indices, yields, and volatility to reveal what actually drives currency movements.

**Current Status**: ✅ Phase 1 (Local Validation) Complete

---

## Prerequisites

### Software Requirements
- Python 3.8+
- Docker (for containerized deployment)
- Java 11+ (for Flink)
- Git

### Python Dependencies
```bash
pip install --break-system-packages -r requirements.txt
```

**requirements.txt:**
```
pandas>=1.3.0
numpy>=1.20.0
yfinance>=0.1.70
requests>=2.26.0
```

---

## Phase 1: Local Validation (COMPLETED ✅)

### What it proves
- ✅ Correlation math is sound (Pearson coefficients, rolling windows)
- ✅ Data ingestion from multiple sources works
- ✅ Regime detection identifies when EUR/USD "follows" different drivers
- ✅ Anomaly detection finds unexplained spikes

### Running Phase 1
```bash
cd /home/claude
python pipeline.py
```

**Output:**
- Console report with correlation heatmaps, regimes, anomalies
- JSON files in `correlation_output/` for downstream processing

---

## Phase 2: Kafka/Flink Streaming Pipeline (NEXT)

### Architecture
```
Data Sources (Real-time)
        ↓
    Kafka Topics (buffering)
        ↓
Flink Jobs (processing)
        ↓
    Time Series DB (InfluxDB/TimescaleDB)
        ↓
REST API + WebSocket
        ↓
React Dashboard
```

### Setup Steps

#### 2.1 Install Flink

**Option A: Local development**
```bash
# Download Flink 1.17
wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
tar -xzf flink-1.17.1-bin-scala_2.12.tgz
cd flink-1.17.1

# Start local cluster
./bin/start-cluster.sh

# Check web UI
# http://localhost:8081
```

**Option B: Docker**
```bash
docker pull flink:1.17
docker run --name flink -p 8081:8081 -p 8082:8082 flink:1.17 jobmanager
docker run --link flink:flink -e JOB_MANAGER_RPC_ADDRESS=flink flink:1.17 taskmanager
```

#### 2.2 Install Kafka

```bash
# Using Docker Compose (recommended)
cat > docker-compose.yml << 'EOF'
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_BROKER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
EOF

docker-compose up -d
```

#### 2.3 Set up Time Series Database

**Using InfluxDB (simplest)**
```bash
docker run -p 8086:8086 influxdb:2.6
# Web UI: http://localhost:8086
```

Or **PostgreSQL + TimescaleDB** for more power:
```bash
docker run -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  timescale/timescaledb:latest-pg14
```

#### 2.4 Create Kafka Topics

```bash
# Using Kafka CLI
kafka-topics --create \
  --topic fx-rates \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics --create \
  --topic commodities \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics --create \
  --topic correlations \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics --create \
  --topic anomalies \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

---

## Phase 3: Build Flink Jobs

See `flink_jobs/` directory:

```
flink_jobs/
├── job_1_normalization.py      # Normalize & enrich
├── job_2_correlation.py        # Rolling correlation analysis
└── job_3_anomaly_detection.py  # Spike detection + news matching
```

### Example: Job 1 (Normalization)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
import json

class NormalizeFunction(MapFunction):
    def map(self, msg):
        data = json.loads(msg)
        # Normalize, validate, enrich
        return json.dumps(normalized_data)

env = StreamExecutionEnvironment.get_execution_environment()
kafka_stream = env.add_source(...)
normalized = kafka_stream.map(NormalizeFunction())
normalized.add_sink(...)
env.execute("Normalization Job")
```

---

## Phase 4: REST API & WebSocket Server

Build a FastAPI app to serve data to the frontend:

```python
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/api/correlations/{window_h}")
async def get_correlations(window_h: int):
    """Get latest correlations for a time window"""
    # Query time series DB
    return correlations_data

@app.websocket("/ws/price-stream")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time price + correlation updates"""
    await websocket.accept()
    while True:
        # Stream data from Kafka
        await websocket.send_json(latest_data)
```

---

## Phase 5: React Frontend Dashboard

(See separate `frontend/` directory with React + D3/Recharts)

### Dashboard Features
- 📊 **Main chart**: EUR/USD price + correlation heatmap overlay
- 🌡️ **Correlation matrix**: Real-time 5x5 heatmap
- 📰 **Event timeline**: News events + economic calendar
- 📈 **Regime waterfall**: Current market driver + yesterday's
- 🚨 **Anomaly alerts**: Unexplained moves

---

## Deployment

### Local Development
```bash
# Terminal 1: Flink
flink-1.17.1/bin/start-cluster.sh

# Terminal 2: Kafka + Services
docker-compose up

# Terminal 3: Python data producer
python data_producer.py

# Terminal 4: API server
python api_server.py

# Terminal 5: Frontend
cd frontend && npm start
```

### Cloud Deployment (AWS)

```bash
# 1. Create EC2 instance
aws ec2 run-instances --image-id ami-0c55b159cbfafe1f0 \
  --instance-type t3.large

# 2. SSH in, clone repo, install dependencies
# 3. Use Docker Compose for services
# 4. Deploy Flink to EMR or managed Flink service

# 5. Deploy API to ECS or AppRunner
# 6. Deploy frontend to S3 + CloudFront
```

### Production Checklist
- [ ] Monitoring (CloudWatch, Datadog, or New Relic)
- [ ] Alerting on failed Flink jobs
- [ ] Data retention policy (how long to keep time series data?)
- [ ] Backup strategy for critical data
- [ ] SSL/TLS for API endpoints
- [ ] API rate limiting
- [ ] Dashboard authentication (optional)

---

## Architecture Decisions

### Why Flink?
- Handles high-frequency tick data (EUR/USD updates every second)
- Perfect time window semantics (essential for correlations)
- Exactly-once processing guarantees
- You know it (shows domain expertise on resume)

### Why Kafka?
- Decouples data sources from processing (resilience)
- Allows replay of historical data
- Multiple consumers (dashboard + analysis + alerts)

### Why Time Series DB?
- Optimized for time-indexed queries
- Fast correlation window lookups
- Natural fit for financial data
- InfluxDB is simple to start; TimescaleDB scales better

---

## Data Sources

All free tiers:

| Source | Symbol | Latency | Notes |
|--------|--------|---------|-------|
| Yahoo Finance (yfinance) | EURUSD=X, CL=F, GC=F, ^GSPC, ^VIX | 15-30 min | No API key needed |
| Alpha Vantage | FX rates | Real-time | Free tier: 5 calls/min |
| ECB | EUR reference rates | Daily | Official, reliable |
| Investing.com | Economic calendar | Real-time | For event correlation |

**Cost**: $0 (free tiers only)

---

## Common Issues & Troubleshooting

### Flink Jobs Failing
```bash
# Check logs
tail -f flink-1.17.1/log/flink-*.log

# Monitor in web UI
# http://localhost:8081
```

### Kafka Connection Errors
```bash
# Verify Kafka is running
docker ps | grep kafka

# Test connection
kafka-console-producer --broker-list localhost:9092 --topic test
```

### Data Gaps
- Free API tiers have rate limits
- Kafka retention policy might be too aggressive
- Time zone misalignment (use UTC everywhere)

---

## Next Steps

1. **Install Phase 2 services** (Kafka, Flink, InfluxDB)
2. **Implement data producer** that pulls from APIs and publishes to Kafka
3. **Implement Flink jobs** (normalization, correlation, anomaly)
4. **Build REST API** for data serving
5. **Deploy frontend** dashboard

---

## Performance Targets

- **Data latency**: 15-30 min (free tier limit)
- **Processing latency**: <5s (Flink end-to-end)
- **Dashboard update**: Real-time (WebSocket)
- **Storage**: 12 months of data (~500GB with all instruments)
- **Cost**: ~$20-50/month on AWS (free tier if new account)

---

## Monitoring & Observability

Once running, track:
- Flink checkpoint latency
- Kafka consumer lag
- Database query performance
- API response times
- Dashboard error rate
- Data completeness (missing ticks)

---

## Questions?

Refer back to the architecture guide: `eur_usd_correlation_architecture.md`
