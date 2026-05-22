# AKSH — Algorithmic Trading & AI Systems

> **Note:** The full AKSH codebase is proprietary (live trading in production — business-critical IP). This repo documents the system architecture and the engineering patterns used to build it.

---

## What is AKSH?

AKSH is a **423,000-line production trading platform** built solo, handling live algorithmic trading on the NSE (National Stock Exchange of India). It is the core infrastructure of [Treum AlgoTech](https://treumalgotech.in).

### Key capabilities

| Component | Description |
|-----------|-------------|
| **Real-time data ingestion** | WebSocket-based live NSE market data pipeline, tick-level with sub-second latency |
| **Signal generation** | Multi-strategy engine: momentum, mean-reversion, VCP patterns |
| **Backtesting (Symphony)** | Walk-forward backtester with vectorized execution and realistic slippage modelling |
| **Live execution** | Order routing via Zerodha Kite API with position sizing and risk controls |
| **Monitoring dashboard** | React + FastAPI real-time P&L, position, and signal dashboard |
| **Data warehouse** | PostgreSQL store with 5+ years of tick + OHLCV data |

### Stats
- **423,000+ lines** of production Python, TypeScript, and SQL
- **Live since 2021** — continuous operation, 4+ years
- **Solo-built** — architecture, infrastructure, trading logic, frontend, and ops
- **Zero external dependencies** on paid data providers (raw NSE feed)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          AKSH Platform                               │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  Market Data  │    │   Signal     │    │   Execution Engine   │  │
│  │   Ingestor   │───▶│  Generator   │───▶│  (Zerodha Kite API)  │  │
│  │  (WebSocket)  │    │  (Multi-Strat│    │  + Risk Controls      │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│          │                   │                       │               │
│          ▼                   ▼                       ▼               │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │               PostgreSQL Data Warehouse                       │  │
│  │   (tick data · OHLCV · signals · orders · positions)         │  │
│  └──────────────────────────────────────────────────────────────┘  │
│          │                   │                       │               │
│          ▼                   ▼                       ▼               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │  Backtesting  │    │  Symphony    │    │   React Dashboard    │  │
│  │   Pipelines  │    │  (Walk-Fwd)  │    │  (FastAPI + WS feed) │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Engineering Patterns (in this repo)

The `/patterns` directory contains **reusable code patterns** extracted from AKSH — the generic engineering decisions, not the proprietary trading logic.

### Python patterns
- [`websocket_market_data_ingestor.py`](patterns/python/websocket_market_data_ingestor.py) — Reconnecting WebSocket ingestor with backpressure
- [`fastapi_streaming_endpoint.py`](patterns/python/fastapi_streaming_endpoint.py) — FastAPI SSE endpoint for real-time dashboard feeds
- [`backtesting_loop.py`](patterns/python/backtesting_loop.py) — Walk-forward backtesting loop with vectorized execution
- [`postgres_time_series_writer.py`](patterns/python/postgres_time_series_writer.py) — High-throughput PostgreSQL time-series batch writer
- [`signal_validation_pipeline.py`](patterns/python/signal_validation_pipeline.py) — Schema-validated signal pipeline with rejection logging

### Multi-agent AI patterns
- [`multi_agent_pipeline.py`](patterns/multi-agent/multi_agent_pipeline.py) — Claude API multi-agent pipeline (research → qualify → act)
- [`structured_output_agent.py`](patterns/multi-agent/structured_output_agent.py) — LLM agent with Pydantic schemas and validation gates
- [`eval_loop.py`](patterns/multi-agent/eval_loop.py) — Continuous eval loop for LLM output quality

---

## Tech Stack

```
Backend:    Python 3.11 · FastAPI · PostgreSQL 15 · asyncio · aiohttp
Data:       pandas · NumPy · psycopg2 · SQLAlchemy (core, not ORM)
Trading:    Zerodha Kite API · NSE WebSocket feed · FinancialDatasets API
Frontend:   React 18 · TypeScript · Recharts · WebSocket
AI/LLM:     Claude API · OpenAI API · LangChain · Pydantic
Infra:      Linux VPS · systemd · git · nginx
```

---

## Other Projects

| Project | Stack | Status |
|---------|-------|--------|
| [EarningsIQ](https://treumalgotech.in) | Python · LangChain · EDGAR API · PostgreSQL | Live — 33K+ filings processed |
| Multi-Agent Lead Gen | Python · Claude API · Pydantic | Live — 80% manual work reduction |
| Growth Gap Fund | Next.js · Supabase · TypeScript | Live |
| Musician's Atelier | Next.js · Supabase | Live |

---

## Contact

**Srijan Arya** — Founder, Treum AlgoTech  
Website: [treumalgotech.in](https://treumalgotech.in)  
Email: srijanaryay@gmail.com  
LinkedIn: [linkedin.com/in/srijan-arya-a0a50693](https://linkedin.com/in/srijan-arya-a0a50693)
