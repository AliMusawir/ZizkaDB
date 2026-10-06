#!/usr/bin/env python3
"""Background worker — embed pending events and upsert to Qdrant."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("embed_worker")

POLL_INTERVAL = float(os.getenv("EMBED_WORKER_POLL_SEC", "2"))
CLAIM_TTL_SECONDS = int(os.getenv("EMBED_WORKER_CLAIM_TTL_SEC", "900"))


async def _claim_pending_batch(pool, limit: int) -> list:
    """Atomically claim a batch so concurrent workers do not process the same events.

    A crashed worker can leave rows in the processing state. Those rows become
    eligible again after the claim TTL, so the queue recovers without manual
    intervention.
    """
    return await pool.fetch(
        """
        WITH candidates AS (
            SELECT event_id
            FROM events
            WHERE index_status IN ('pending', 'failed')
               OR (
                    index_status = 'processing'
                    AND (
                        index_claimed_at IS NULL
                        OR index_claimed_at < NOW() - ($2 * INTERVAL '1 second')
                    )
               )
            ORDER BY timestamp ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE events AS e
        SET index_status = 'processing',
            index_claimed_at = NOW()
        FROM candidates
        WHERE e.event_id = candidates.event_id
        RETURNING e.event_id, e.tenant_id, e.agent_id, e.event_type, e.data
        """,
        limit,
        CLAIM_TTL_SECONDS,
    )


async def process_pending_batch(limit: int = 50) -> int:
    from db.connection import get_pool, get_qdrant
    from services.embeddings import generate_embedding, event_to_text
    from services.event_write import _pgvector_literal
    from qdrant_client.models import PointStruct

    pool = get_pool()
    rows = await _claim_pending_batch(pool, limit)

    processed = 0
    for row in rows:
        event_id = str(row["event_id"])
        data = row["data"]
        if isinstance(data, str):
            data = json.loads(data)
        try:
            text = event_to_text(row["event_type"], dict(data))
            embedding = await generate_embedding(text, str(row["tenant_id"]))
            if not embedding:
                raise RuntimeError("no embedding returned")

            qdrant = get_qdrant()
            await qdrant.upsert(
                collection_name="agent_events",
                points=[
                    PointStruct(
                        id=event_id,
                        vector=embedding,
                        payload={
                            "tenant_id": str(row["tenant_id"]),
                            "agent_id": row["agent_id"],
                            "event_type": row["event_type"],
                        },
                    )
                ],
            )
            await pool.execute(
                """
                UPDATE events
                SET embedding = $1::vector,
                    index_status = 'indexed',
                    index_claimed_at = NULL
                WHERE event_id = $2
                """,
                _pgvector_literal(embedding),
                row["event_id"],
            )
            processed += 1
        except Exception as exc:
            logger.warning("index failed for %s: %s", event_id, exc)
            await pool.execute(
                """
                UPDATE events
                SET index_status = 'failed',
                    index_claimed_at = NULL
                WHERE event_id = $1
                """,
                row["event_id"],
            )
    return processed


async def main() -> None:
    from db.connection import init_db, close_db

    await init_db()
    logger.info("embed worker started")
    try:
        while True:
            n = await process_pending_batch()
            if n:
                logger.info("indexed %d events", n)
            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await close_db()


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    asyncio.run(main())
