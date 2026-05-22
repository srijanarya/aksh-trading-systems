# AKSH System Architecture

## High-Level Design

AKSH is built around a **streaming data pipeline** that flows from raw market data through signal generation to live order execution. Each layer is independently scalable and failure-isolated.

```
                      ┌────────────────────────────────────────────────────┐
                      │                  NSE / Exchange                    │
                      │         WebSocket feed (tick-level data)           │
                      └─────────────────────┬──────────────────────────────┘
                                            │
                                            ▼
                      ┌────────────────────────────────────────────────────┐
                      │           Market Data Ingestor (Python)            │
                      │  • Reconnecting WebSocket client                   │
                      │  • Binary frame decoder (exchange protocol)        │
                      │  • Tick normaliser → typed Tick objects            │
                      │  • Backpressure queue (asyncio)                    │
                      └───────────────┬───────────────────┬────────────────┘
                                      │                   │
                          Async write │                   │ Fan-out
                                      ▼                   ▼
              ┌──────────────────────────┐   ┌───────────────────────────┐
              │  PostgreSQL Data Store   │   │   Signal Engine (Python)  │
              │  • Raw tick archive      │   │  • Multi-strategy runner  │
              │  • OHLCV materialisation │   │  • Strategy 1: Momentum   │
              │  • Partition by date     │   │  • Strategy 2: Mean-rev   │
              │  • TimescaleDB hypertable│   │  • Strategy N: VCP        │
              └──────────────────────────┘   └────────────┬──────────────┘
                                                          │
                                              Signal pass │ validation
                                                          ▼
                                         ┌───────────────────────────────┐
                                         │    Risk & Position Manager    │
                                         │  • Position size calculator   │
                                         │  • Exposure limits            │
                                         │  • Daily loss circuit breaker │
                                         │  • Slippage estimator         │
                                         └───────────────┬───────────────┘
                                                         │
                                                         ▼
                                         ┌───────────────────────────────┐
                                         │   Execution Engine            │
                                         │   (Zerodha Kite API)          │
                                         │  • Order routing              │
                                         │  • Fill tracking              │
                                         │  • Retry on partial fill      │
                                         └───────────────────────────────┘

```

## Backtesting (Symphony)

```
Historical OHLCV (PostgreSQL)
        │
        ▼
┌───────────────────────────────────────────────────────────────────┐
│                    Symphony Backtesting Engine                    │
│                                                                   │
│  Date range split                                                 │
│  ┌──────────────────────────────┐                                 │
│  │  Train window  │  Test window│  ← Walk-forward fold           │
│  └──────────────────────────────┘                                 │
│         │                │                                        │
│         ▼                ▼                                        │
│  [Signal params]  [Out-of-sample P&L]                             │
│                          │                                        │
│                          ▼                                        │
│              Slippage / commission model                          │
│              Position sizing (Kelly / fixed fraction)             │
│              Drawdown and Sharpe calculation                      │
└───────────────────────────────────────────────────────────────────┘
        │
        ▼
  Report: per-strategy metrics, parameter sensitivity, forward walk summary
```

## Dashboard (React + FastAPI)

```
  Browser (React 18)
       │  SSE (EventSource)
       │
       ▼
  FastAPI SSE endpoint  ──── BroadcastHub ──── Position updates (async)
       │                                              │
       │                                              ▼
       │                                       Signal Engine events
       ▼
  Real-time views:
  • Live P&L chart (Recharts)
  • Open positions table
  • Signal feed (last 50 signals)
  • Risk metrics (exposure, daily loss)
  • Order history
```

## Reliability Design

| Concern | Approach |
|---------|----------|
| WebSocket disconnect | Exponential backoff reconnect, state recovery on reconnect |
| Database write lag | Async batch writer, COPY protocol, dead-letter file on failure |
| Exchange API rate limits | Token bucket rate limiter per endpoint |
| Signal fan-out slowdown | Per-strategy goroutine (Python asyncio Task) |
| Data gaps | Gap detector with NSE calendar — flags missing sessions |
| Market hours | State machine: PRE_OPEN → OPEN → POST_CLOSE |

## Key Numbers

| Metric | Value |
|--------|-------|
| Codebase size | 423,000+ lines |
| Tick throughput | ~2,000-5,000 ticks/sec during peak NSE hours |
| Strategies in production | Multiple (configuration-driven) |
| Data history | 5+ years of NSE tick data |
| Latency (signal to order) | <200ms (Python + async, not HFT) |
| Uptime | Daily (market hours) — automated start/stop via systemd |
