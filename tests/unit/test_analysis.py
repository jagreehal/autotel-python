"""Cohort analysis helpers."""

from autotel.analysis import bucket, compare_cohorts


def test_bucket_ranges() -> None:
    assert bucket(500, [1000, 2000, 5000]) == "<1000"
    assert bucket(1500, [1000, 2000, 5000]) == "1000-2000"
    assert bucket(9000, [1000, 2000, 5000]) == ">=5000"
    assert bucket(float("nan"), [1000]) == "unknown"


def test_compare_cohorts_finds_separator() -> None:
    outlier = [
        {"account.shard": "legacy-core", "withdrawal.amount_band": "2000-5000"},
        {"account.shard": "legacy-core", "withdrawal.amount_band": "2000-5000"},
        {"account.shard": "legacy-core", "withdrawal.amount_band": "2000-5000"},
    ]
    baseline = [
        {"account.shard": "modern", "withdrawal.amount_band": "2000-5000"},
        {"account.shard": "modern", "withdrawal.amount_band": "1000-2000"},
        {"account.shard": "modern", "withdrawal.amount_band": "2000-5000"},
    ]
    results = compare_cohorts(outlier=outlier, baseline=baseline)
    assert results
    assert results[0].field == "account.shard"
    assert results[0].value == "legacy-core"
    assert results[0].difference > 0
