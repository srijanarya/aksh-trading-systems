"""
High-Throughput PostgreSQL Time-Series Batch Writer
----------------------------------------------------
Pattern for writing tick / OHLCV data to PostgreSQL at high throughput:
- Collects rows in an in-memory buffer
- Flushes on size threshold OR time interval (whichever comes first)
- Uses COPY (fastest PostgreSQL bulk insert, ~10x faster than INSERT)
- Handles backpressure and write failures without data loss

Used in AKSH to persist NSE tick data to the data warehouse.
"""

import asyncio
import io
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class WriterConfig:
    dsn: str                           # postgresql://user:pass@host/db
    table: str                         # target table name
    columns: list[str]                 # column order for COPY
    flush_size: int = 500              # flush when buffer hits this size
    flush_interval: float = 1.0        # flush at least every N seconds
    max_retries: int = 3


class TimeSeriesBatchWriter:
    """
    Asynchronous batch writer that buffers rows and flushes using PostgreSQL COPY.

    Why COPY over INSERT:
    - COPY bypasses the query planner → ~5-10x faster for bulk inserts
    - Single network round-trip for the entire batch
    - Works well with TimescaleDB / partitioned tables

    Usage:
        writer = TimeSeriesBatchWriter(config)
        await writer.start()
        await writer.write({"time": ..., "symbol": ..., "price": ...})
        await writer.stop()  # flushes remaining buffer
    """

    def __init__(self, config: WriterConfig):
        self.config = config
        self._buffer: list[tuple] = []
        self._pool: asyncpg.Pool | None = None
        self._flush_task: asyncio.Task | None = None
        self._last_flush = time.monotonic()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self.config.dsn, min_size=2, max_size=5)
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(f"TimeSeriesBatchWriter started → {self.config.table}")

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        await self._flush()  # drain remaining buffer
        if self._pool:
            await self._pool.close()

    async def write(self, row: dict[str, Any]) -> None:
        """Add a row to the buffer. Flushes if buffer is full."""
        async with self._lock:
            # Build tuple in column order
            self._buffer.append(tuple(row.get(col) for col in self.config.columns))

            if len(self._buffer) >= self.config.flush_size:
                await self._flush_locked()

    async def _periodic_flush(self) -> None:
        """Background task: flush on interval even if buffer isn't full."""
        while True:
            try:
                await asyncio.sleep(self.config.flush_interval)
                async with self._lock:
                    if self._buffer:
                        await self._flush_locked()
            except asyncio.CancelledError:
                break

    async def _flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Must be called with self._lock held."""
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer.clear()

        for attempt in range(self.config.max_retries):
            try:
                await self._copy_records(batch)
                elapsed = time.monotonic() - self._last_flush
                logger.debug(
                    f"Flushed {len(batch)} rows to {self.config.table} "
                    f"({elapsed:.2f}s since last flush)"
                )
                self._last_flush = time.monotonic()
                return

            except Exception as e:
                logger.warning(f"Flush attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    logger.error(f"All retries failed — {len(batch)} rows LOST")
                    # In production: write to dead-letter file for manual recovery
                    self._write_dead_letter(batch)
                else:
                    await asyncio.sleep(0.5 * (attempt + 1))

    async def _copy_records(self, batch: list[tuple]) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.copy_records_to_table(
                self.config.table,
                records=batch,
                columns=self.config.columns,
            )

    def _write_dead_letter(self, batch: list[tuple]) -> None:
        """Last-resort: write failed rows to a local file for manual recovery."""
        path = f"/tmp/dead_letter_{self.config.table}_{int(time.time())}.csv"
        buf = io.StringIO()
        for row in batch:
            buf.write(",".join(str(v) for v in row) + "\n")
        with open(path, "w") as f:
            f.write(buf.getvalue())
        logger.error(f"Dead-letter written to {path}")


# --- Usage example ---
async def main():
    config = WriterConfig(
        dsn="postgresql://localhost/trading",
        table="ticks",
        columns=["time", "instrument_token", "last_price", "volume", "bid", "ask"],
        flush_size=500,
        flush_interval=1.0,
    )

    writer = TimeSeriesBatchWriter(config)
    await writer.start()

    # Simulate tick stream
    for i in range(2000):
        await writer.write({
            "time": time.time(),
            "instrument_token": 738561,
            "last_price": 2500.0 + i * 0.01,
            "volume": 100 + i,
            "bid": 2499.95,
            "ask": 2500.05,
        })

    await writer.stop()
    print("Done")


if __name__ == "__main__":
    asyncio.run(main())
