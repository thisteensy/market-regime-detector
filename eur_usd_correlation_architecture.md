# EUR/USD Streaming Correlation Dashboard
## Architecture & Implementation Guide

---

## Project Overview

A real-time streaming dashboard that reveals what actually drives EUR/USD exchange rates by correlating currency movements with commodities, indices, yields, and news events.

**Why this matters**: Most people see exchange rates as random noise. This project shows the structure—the underlying drivers that move markets in real-time.

---

## Data Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES (Real-time)                 │
├─────────────────────────────────────────────────────────────┤
│ • Exchange Rates (EUR/USD, GBP/USD, etc.)                   │
│ • Commodities (Oil, Gold, Natural Gas)                      │
│ • Stock Indices (S&P 500, DAX, STOXX Europe)               │
│ • Bond Yields (10Y Treasury, German Bund)                   │
│ • Volatility (VIX, VSTOXX)                                  │
│ • News Events (Reuters, Bloomberg, Economic calendars)      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              KAFKA / MESSAGE QUEUE (Buffering)               │
│         (Topics: fx-rates, commodities, yields, news)       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    FLINK STREAMING JOBS                      │
├─────────────────────────────────────────────────────────────┤
│ Job 1: Normalization & Enrichment                           │
│   - Normalize all inputs to common format                   │
│   - Add metadata (source, timestamp, confidence)            │
│   - Deduplicate & handle late arrivals                      │
│                                                              │
│ Job 2: Time-window Correlation Analysis                     │
│   - 1-hour, 4-hour, daily windows                           │
│   - Calculate correlation coefficients (Pearson)           │
│   - Detect correlation regime changes                       │
│   - Flag anomalies (unexplained moves)                      │
│                                                              │
│ Job 3: Event Detection & Causality                          │
│   - Detect significant spikes (>2σ deviation)              │
│   - Match spikes to news events (time window)               │
│   - Assign confidence scores                                │
│   - Generate alerts                                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            TIME SERIES DATABASE (InfluxDB/TimescaleDB)       │
│         (Stores: raw prices, correlations, events)          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              REST API / WebSocket Server                     │
│         (Real-time data delivery to frontend)               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            FRONTEND DASHBOARD (React + D3/Recharts)         │
│         (Visualization, exploration, real-time updates)     │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Sources & APIs

### Exchange Rates
- **Primary**: Alpha Vantage (free tier), Finnhub, or IEX Cloud
- **Frequency**: Every 5-60 seconds depending on tier
- **Pairs to track**: EUR/USD (primary), GBP/USD, USD/JPY

### Commodities
- **Oil**: Rapid API or commodity exchange APIs
- **Gold**: Finnhub or commodities APIs
- **Natural Gas**: US EIA data

### Indices & Yields
- **Stock Indices**: Yahoo Finance API (free), Finnhub
- **Bond Yields**: US Treasury, ECB, German Bund data
- **Volatility**: VIX data (Yahoo Finance), VSTOXX

### News
- **NewsAPI.org** (good for headlines + sentiment)
- **Reuters/Bloomberg API** (if available)
- **Economic Calendar**: Investing.com or Fred API for scheduled events

### Data Quality Considerations
- **Latency**: Market data might be 15-30 min delayed on free tiers
- **Gaps**: Handle missing data gracefully
- **Duplicates**: Kafka ensures exactly-once semantics

---

## Flink Job Details

### Job 1: Normalization & Enrichment

```python
# Pseudocode
stream = env.add_source(KafkaSource(...))

normalized = stream \
    .map(lambda msg: normalize(msg)) \
    .filter(lambda x: validate(x)) \
    .assign_timestamps_and_watermarks(...)

# Output to Kafka topic: normalized-data
```

**What it does:**
- Converts all inputs to standard schema (timestamp, symbol, value, source)
- Validates data quality (range checks, outlier detection)
- Assigns event time & watermarks (handles late data)
- Filters duplicates

### Job 2: Correlation Analysis

```python
# Pseudocode - simplified
normalized = env.add_source(KafkaSource(...))

# Group by symbol, apply windowing
correlations = normalized \
    .key_by(lambda x: "eur_usd") \
    .window(SlidingEventTimeWindow(hours=4, hours=1)) \
    .aggregate(CorrelationAggregator()) \
    .map(lambda x: compute_correlations(x))

# Output: correlation matrices, anomalies
```

**What it calculates:**
- Correlation of EUR/USD vs. each commodity/index
- Rolling correlations (4-hour windows, 1-hour slides)
- Correlation strength changes (when correlations break down)
- Confidence scores based on data completeness

**Output schema:**
```json
{
  "timestamp": "2025-03-16T10:30:00Z",
  "eur_usd_price": 1.0850,
  "correlations": {
    "oil": 0.87,
    "gold": 0.42,
    "sp500": -0.15,
    "vix": 0.65,
    "bund_yield": 0.72
  },
  "regime": "oil_following",
  "confidence": 0.89
}
```

### Job 3: Event Detection & Causality

```python
# Detect spikes and correlate with news
prices = env.add_source(KafkaSource(...))
news = env.add_source(NewsSource(...))

# Detect anomalies
spikes = prices \
    .key_by(lambda x: x.symbol) \
    .window(SlidingEventTimeWindow(minutes=30, minutes=5)) \
    .aggregate(AnomalyDetector(zscore_threshold=2.0))

# Join with news (30-min window)
correlated = spikes \
    .connect(news) \
    .co_flat_map(lambda spike, news_events: match_spike_to_news(spike, news_events))
```

**Output:**
```json
{
  "timestamp": "2025-03-16T14:15:00Z",
  "event_type": "spike",
  "symbol": "eur_usd",
  "magnitude": 0.035,
  "likely_cause": "ECB interest rate decision",
  "confidence": 0.92,
  "relevant_headlines": [
    "ECB Hikes Rates to 4.25% in Surprise Move",
    "Euro Strengthens Against Dollar on Rate Expectations"
  ]
}
```

---

## Frontend Dashboard Concept

### Key Visualizations

1. **Main Chart: EUR/USD Price + Correlations**
   - Time series of EUR/USD (large, primary)
   - Overlay: correlation strength heatmap background
   - Shows which period had which correlations

2. **Correlation Matrix Heatmap**
   - Real-time updating 5x5 matrix
   - EUR/USD vs. [Oil, Gold, S&P500, VIX, Bund Yield]
   - Color intensity = correlation strength
   - Changes highlight regime shifts

3. **Event Timeline**
   - News events + economic calendar on timeline
   - Annotate on price chart where events caused moves
   - Click to see full headline + impact assessment

4. **Waterfall: "What's Moving EUR/USD Today?"**
   - Shows: "Currently following Oil (+0.87 corr), was Gold yesterday (+0.65)"
   - Explains current market regime
   - Shows correlation changes

5. **Anomaly Alerts**
   - Unexplained moves (low correlation to known drivers)
   - "This 1.5% spike doesn't match Oil, yields, or news"
   - Sparks investigation

### Design Direction
- **Aesthetic**: Clean, data-focused, slightly minimalist with strategic use of color
- **Tone**: Professional but approachable (this is for people who care about FX, not just traders)
- **Key interaction**: Time selection, hover for details, click news to expand

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1-2)
- [ ] Set up Flink development environment
- [ ] Build data ingestion from 2-3 sources (EUR/USD, Oil, S&P500)
- [ ] Implement normalization job
- [ ] Get raw data flowing into time series DB

### Phase 2: Core Analysis (Week 2-3)
- [ ] Implement correlation calculation job
- [ ] Validate correlation math against known values
- [ ] Add windowing (1h, 4h, daily)
- [ ] Test with historical data

### Phase 3: Event Detection (Week 3-4)
- [ ] Build spike detection algorithm
- [ ] Integrate news feed
- [ ] Implement spike-to-news matching
- [ ] Generate alerts

### Phase 4: Frontend (Week 4-5)
- [ ] Build React dashboard skeleton
- [ ] Implement real-time chart (EUR/USD price)
- [ ] Add correlation heatmap
- [ ] Connect to backend via WebSocket/REST

### Phase 5: Polish & Deploy (Week 5-6)
- [ ] Performance optimization (Flink tuning, DB queries)
- [ ] Add historical replay capability (learn from past events)
- [ ] Deploy to cloud (AWS, Heroku, or similar)
- [ ] Document architecture & lessons learned

---

## Technical Decisions

### Why Flink?
- Handles high-frequency streaming data (exchange rates tick constantly)
- Excellent time window semantics (perfect for correlation windows)
- Fault tolerance (important for continuous operation)
- You know it, and it shows domain expertise

### Why Kafka?
- Decouples data sources from processing
- Allows replay of historical data
- Supports multiple consumers (dashboard + other analysis)

### Why Time Series DB?
- Optimized for time-indexed queries (fast correlation lookups)
- Natural fit for financial data
- Options: InfluxDB (simple, free tier), TimescaleDB (PostgreSQL-based, powerful)

### Frontend Library Choice
- React for interactivity
- Recharts or D3.js for charting
- WebSocket for real-time updates

---

## Open Questions to Resolve

1. **Data Quality**: Which APIs are reliable enough? What's your budget?
2. **Latency**: Can you tolerate 15-30 min delay on free tiers, or do you need real-time?
3. **Storage**: How long to keep data? (1 month? 1 year?)
4. **Scaling**: Start local, or deploy to cloud from day one?
5. **News matching**: How fuzzy should spike-to-news matching be? (exact time vs. +/- 30 min window)

---

## Why This Project Stands Out

✓ **Domain expertise**: You understand FX markets because you live with them  
✓ **Real streaming**: Not a toy project—handles real market data  
✓ **Practical**: You could actually *use* this tool  
✓ **Technically sophisticated**: Flink, windowing, anomaly detection, event correlation  
✓ **Memorable**: Someone hiring will remember this, not forget it immediately  
✓ **Extensible**: Easy to add more instruments, more analysis, more visualization  

This is the kind of project that can be a GitHub star-getter if you build it well and document it clearly.

---

## Next Steps

1. **Pick a data source pair**: Choose which API you'll use for exchange rates + one commodity
2. **Local prototype**: Build the correlation math first (pandas), validate it works
3. **Flink skeleton**: Set up basic data ingestion into Flink
4. **Iterate**: Get comfortable with the pipeline before adding complexity

Ready to start coding?
