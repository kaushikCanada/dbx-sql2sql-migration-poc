"""
Test 3 — PK Reservation

Validates that reserve_pk_block correctly assigns contiguous, non-overlapping
identity blocks so migrated rows land on pre-known PKs without colliding with
each other or with live inserts after migration.
"""
import threading
import pytest
from src.connections import target_conn
from src.target_ops import reserve_pk_block


@pytest.fixture
def tgt():
    conn = target_conn()
    yield conn
    conn.close()


def test_reserve_returns_positive_int(tgt):
    first_id = reserve_pk_block(tgt, "clients", 5)
    assert isinstance(first_id, int)
    assert first_id > 0


def test_sequential_reservations_dont_overlap(tgt):
    first  = reserve_pk_block(tgt, "clients", 3)
    second = reserve_pk_block(tgt, "clients", 3)
    # Second block must start exactly where first block ended
    assert second >= first + 3


def test_zero_block_size_raises(tgt):
    with pytest.raises(ValueError):
        reserve_pk_block(tgt, "clients", 0)


def test_concurrent_reservations_dont_overlap():
    results = []
    errors  = []

    def reserve():
        try:
            conn = target_conn(autocommit=True)
            fid  = reserve_pk_block(conn, "clients", 10)
            results.append(fid)
            conn.close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=reserve) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors during concurrent reservation: {errors}"
    assert len(results) == 5

    sorted_ids = sorted(results)
    for i in range(len(sorted_ids) - 1):
        assert sorted_ids[i + 1] >= sorted_ids[i] + 10, (
            f"Overlap detected: block starting at {sorted_ids[i]} "
            f"overlaps with block starting at {sorted_ids[i+1]}"
        )
