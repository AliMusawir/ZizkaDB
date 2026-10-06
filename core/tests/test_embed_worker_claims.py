import asyncio

from workers import embed_worker


class FakePool:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return self.rows


def test_claim_pending_batch_uses_skip_locked_and_processing_state(monkeypatch):
    monkeypatch.setattr(embed_worker, "CLAIM_TTL_SECONDS", 123)
    pool = FakePool(rows=[{"event_id": "1"}])

    rows = asyncio.run(embed_worker._claim_pending_batch(pool, 7))

    assert rows == [{"event_id": "1"}]
    assert len(pool.calls) == 1

    query, args = pool.calls[0]
    normalized = " ".join(query.split())

    assert "FOR UPDATE SKIP LOCKED" in normalized
    assert "index_status = 'processing'" in normalized
    assert "index_claimed_at = NOW()" in normalized
    assert "index_status IN ('pending', 'failed')" in normalized
    assert "index_claimed_at < NOW() - ($2 * INTERVAL '1 second')" in normalized
    assert args == (7, 123)


def test_claim_pending_batch_reclaims_stale_processing_rows():
    pool = FakePool()

    asyncio.run(embed_worker._claim_pending_batch(pool, 50))

    query, _ = pool.calls[0]
    normalized = " ".join(query.split())

    assert "index_status = 'processing'" in normalized
    assert "index_claimed_at IS NULL" in normalized
    assert "index_claimed_at < NOW()" in normalized
