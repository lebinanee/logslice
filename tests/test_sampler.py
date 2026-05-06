"""Tests for logslice.sampler."""

import pytest

from logslice.sampler import sample_by_field_hash, sample_every_nth, sample_random


def make_entries(n: int) -> list[dict]:
    return [{"id": i, "msg": f"log {i}"} for i in range(n)]


# ---------------------------------------------------------------------------
# sample_random
# ---------------------------------------------------------------------------

def test_sample_random_rate_zero_yields_nothing():
    entries = make_entries(100)
    result = list(sample_random(entries, rate=0.0, seed=42))
    assert result == []


def test_sample_random_rate_one_yields_all():
    entries = make_entries(50)
    result = list(sample_random(entries, rate=1.0, seed=0))
    assert result == entries


def test_sample_random_approximate_rate(seed=7):
    entries = make_entries(10_000)
    result = list(sample_random(entries, rate=0.1, seed=seed))
    assert 800 <= len(result) <= 1200, f"Unexpected count: {len(result)}"


def test_sample_random_reproducible():
    entries = make_entries(200)
    r1 = list(sample_random(entries, rate=0.3, seed=99))
    r2 = list(sample_random(entries, rate=0.3, seed=99))
    assert r1 == r2


def test_sample_random_invalid_rate():
    with pytest.raises(ValueError, match="rate"):
        list(sample_random([], rate=1.5))
    with pytest.raises(ValueError, match="rate"):
        list(sample_random([], rate=-0.1))


# ---------------------------------------------------------------------------
# sample_every_nth
# ---------------------------------------------------------------------------

def test_sample_every_nth_returns_first_entry():
    entries = make_entries(10)
    result = list(sample_every_nth(entries, n=3))
    assert result[0] == entries[0]


def test_sample_every_nth_correct_indices():
    entries = make_entries(9)
    result = list(sample_every_nth(entries, n=3))
    assert result == [entries[0], entries[3], entries[6]]


def test_sample_every_nth_n1_yields_all():
    entries = make_entries(20)
    assert list(sample_every_nth(entries, n=1)) == entries


def test_sample_every_nth_invalid_n():
    with pytest.raises(ValueError, match="n must be"):
        list(sample_every_nth([], n=0))


# ---------------------------------------------------------------------------
# sample_by_field_hash
# ---------------------------------------------------------------------------

def test_sample_by_field_hash_deterministic():
    entries = [{"user_id": str(i)} for i in range(500)]
    r1 = list(sample_by_field_hash(entries, field="user_id", rate=0.4))
    r2 = list(sample_by_field_hash(entries, field="user_id", rate=0.4))
    assert r1 == r2


def test_sample_by_field_hash_same_value_always_same_decision():
    entry = {"user_id": "abc123"}
    entries = [entry] * 50
    results = list(sample_by_field_hash(entries, field="user_id", rate=0.5))
    assert len(results) == 0 or len(results) == 50


def test_sample_by_field_hash_rate_zero():
    entries = [{"user_id": str(i)} for i in range(100)]
    result = list(sample_by_field_hash(entries, field="user_id", rate=0.0))
    assert result == []


def test_sample_by_field_hash_invalid_rate():
    with pytest.raises(ValueError, match="rate"):
        list(sample_by_field_hash([], field="x", rate=2.0))
