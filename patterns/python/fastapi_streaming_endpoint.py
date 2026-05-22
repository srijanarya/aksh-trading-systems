"""
FastAPI Server-Sent Events (SSE) Streaming Endpoint
----------------------------------------------------
Real-time dashboard feed pattern used in AKSH's trading monitor:
- SSE over HTTP/1.1 (simpler than WebSocket for one-way server→browser)
- Per-client asyncio.Queue so slow clients don't block each other
- Heartbeat to keep connections alive through proxies/load balancers
- Clean disconnection handling (generators stop on client disconnect)

Used in AKSH to push live P&L, positions, and signals to the React dashboard.
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse


# --- Domain model (example) ------------------------------------------------

@dataclass
class PositionUpdate:
    symbol: str
    quantity: int
    avg_price: float
    last_price: float
    pnl: float
    timestamp: float


# --- Broadcast hub ---------------------------------------------------------

class BroadcastHub:
    """
    Fan-out: one producer → N per-client queues.
    Each SSE client gets its own queue so a slow client never blocks others.
    """

    def __init__(self, queue_max_size: int = 500):
        self._clients: set[asyncio.Queue] = set()
        self._queue_max_size = queue_max_size

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_max_size)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._clients.discard(q)

    async def publish(self, event: dict) -> None:
        dead = set()
        for q in self._clients:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.add(q)  # slow client — drop and disconnect

        for q in dead:
            self._clients.discard(q)

    @property
    def client_count(self) -> int:
        return len(self._clients)


hub = BroadcastHub()


# --- Fake market data producer (replace with real feed in production) ------

async def market_data_producer() -> None:
    """Simulates a market data feed pushing updates to the hub."""
    import random

    symbols = ["RELIANCE", "TCS", "INFY", "HDFC"]
    prices = {s: 2000.0 + random.random() * 1000 for s in symbols}

    while True:
        symbol = random.choice(symbols)
        prices[symbol] *= 1 + (random.random() - 0.5) * 0.002

        update = PositionUpdate(
            symbol=symbol,
            quantity=random.randint(1, 100),
            avg_price=prices[symbol] * 0.995,
            last_price=prices[symbol],
            pnl=(prices[symbol] - prices[symbol] * 0.995) * random.randint(1, 100),
            timestamp=time.time(),
        )
        await hub.publish(asdict(update))
        await asyncio.sleep(0.1)  # 10 updates/sec


# --- FastAPI app -----------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(market_data_producer())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


async def sse_generator(
    request: Request,
    client_queue: asyncio.Queue,
    heartbeat_interval: float = 15.0,
) -> AsyncGenerator[str, None]:
    """
    Yields SSE-formatted strings until the client disconnects.
    Sends a heartbeat comment every `heartbeat_interval` seconds
    to prevent proxy timeouts.
    """
    last_heartbeat = time.monotonic()

    try:
        while True:
            # Check if client has disconnected
            if await request.is_disconnected():
                break

            try:
                # Wait for next event (with timeout for heartbeat)
                timeout = max(0.1, heartbeat_interval - (time.monotonic() - last_heartbeat))
                event = await asyncio.wait_for(client_queue.get(), timeout=timeout)
                yield f"data: {json.dumps(event)}\n\n"

            except asyncio.TimeoutError:
                # Send SSE heartbeat comment
                yield ": heartbeat\n\n"
                last_heartbeat = time.monotonic()

    except asyncio.CancelledError:
        pass
    finally:
        hub.unsubscribe(client_queue)


@app.get("/stream/positions")
async def stream_positions(request: Request) -> StreamingResponse:
    """
    SSE endpoint — connect once, receive a continuous stream of position updates.

    Usage (JavaScript):
        const es = new EventSource('/stream/positions');
        es.onmessage = (e) => console.log(JSON.parse(e.data));
    """
    client_queue = hub.subscribe()
    return StreamingResponse(
        sse_generator(request, client_queue),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
            "Connection": "keep-alive",
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "connected_clients": hub.client_count}
