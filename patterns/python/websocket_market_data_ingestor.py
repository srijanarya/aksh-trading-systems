"""
WebSocket Market Data Ingestor Pattern
--------------------------------------
Reconnecting WebSocket client with:
- Exponential backoff on disconnect
- Backpressure via asyncio.Queue with bounded size
- Heartbeat / ping-pong to detect stale connections
- Structured tick normalization

Used in AKSH for live NSE market data ingestion.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


@dataclass
class Tick:
    instrument_token: int
    last_price: float
    volume: int
    timestamp: float
    bid: float
    ask: float


class ReconnectingWebSocketIngestor:
    """
    Resilient WebSocket client that:
    1. Reconnects with exponential backoff on any disconnect
    2. Normalizes raw exchange payloads into typed Tick objects
    3. Pushes to a bounded async queue for downstream consumers
    4. Tracks heartbeat health to detect zombie connections
    """

    MAX_RECONNECT_DELAY = 60.0   # seconds
    INITIAL_RECONNECT_DELAY = 1.0
    QUEUE_MAX_SIZE = 5000        # backpressure: drop old if full
    HEARTBEAT_INTERVAL = 30.0   # ping every 30s

    def __init__(
        self,
        uri: str,
        on_tick: Optional[Callable[[Tick], None]] = None,
        instruments: Optional[list[int]] = None,
    ):
        self.uri = uri
        self.on_tick = on_tick
        self.instruments = instruments or []
        self.queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self._running = False
        self._last_heartbeat = 0.0

    async def start(self) -> None:
        self._running = True
        delay = self.INITIAL_RECONNECT_DELAY

        while self._running:
            try:
                logger.info(f"Connecting to {self.uri}")
                async with websockets.connect(
                    self.uri,
                    ping_interval=self.HEARTBEAT_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    delay = self.INITIAL_RECONNECT_DELAY  # reset on successful connect
                    await self._on_connect(ws)
                    await self._listen(ws)

            except (ConnectionClosed, WebSocketException) as e:
                logger.warning(f"WebSocket disconnected: {e}. Reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.MAX_RECONNECT_DELAY)

            except asyncio.CancelledError:
                logger.info("Ingestor cancelled — stopping")
                break

            except Exception as e:
                logger.error(f"Unexpected error: {e}. Reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.MAX_RECONNECT_DELAY)

    async def stop(self) -> None:
        self._running = False

    async def _on_connect(self, ws) -> None:
        """Subscribe to instruments on connect."""
        if self.instruments:
            subscribe_msg = json.dumps({
                "a": "subscribe",
                "v": self.instruments,
            })
            await ws.send(subscribe_msg)
            logger.info(f"Subscribed to {len(self.instruments)} instruments")

    async def _listen(self, ws) -> None:
        async for raw_message in ws:
            try:
                tick = self._parse(raw_message)
                if tick is None:
                    continue

                # Backpressure: if queue full, drop the oldest tick
                if self.queue.full():
                    try:
                        self.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass

                await self.queue.put(tick)

                if self.on_tick:
                    self.on_tick(tick)

                self._last_heartbeat = time.monotonic()

            except Exception as e:
                logger.error(f"Tick parse error: {e}")

    def _parse(self, raw: bytes | str) -> Optional[Tick]:
        """
        Normalize exchange payload → Tick.
        Exchange-specific parsing goes here; keep downstream code clean.
        """
        try:
            if isinstance(raw, bytes):
                # Binary protocol — implement exchange-specific binary unpacking here
                # e.g., struct.unpack for Zerodha Kite binary frames
                return None  # replace with actual binary parse

            data = json.loads(raw)
            if data.get("type") != "tick":
                return None

            return Tick(
                instrument_token=data["token"],
                last_price=float(data["last_price"]),
                volume=int(data["volume"]),
                timestamp=float(data.get("timestamp", time.time())),
                bid=float(data.get("depth", {}).get("buy", [{}])[0].get("price", 0)),
                ask=float(data.get("depth", {}).get("sell", [{}])[0].get("price", 0)),
            )
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logger.debug(f"Unrecognised frame: {e}")
            return None

    async def consume(self) -> Tick:
        """Pull one tick from the queue (for downstream consumers)."""
        return await self.queue.get()


# --- Usage example ---
async def main():
    ingestor = ReconnectingWebSocketIngestor(
        uri="wss://your-exchange-ws-endpoint",
        instruments=[738561, 256265],  # NSE instrument tokens
    )

    async def writer():
        while True:
            tick = await ingestor.consume()
            print(f"{tick.instrument_token} @ {tick.last_price} vol={tick.volume}")

    await asyncio.gather(ingestor.start(), writer())


if __name__ == "__main__":
    asyncio.run(main())
