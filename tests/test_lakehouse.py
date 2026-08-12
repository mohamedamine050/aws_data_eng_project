"""Unit tests for the data-lake layout.

Path resolution is the kind of code that looks too simple to test right up
until a prefix typo writes a day of data into the wrong zone. These are pure
string functions, so the whole medallion layout is covered without a bucket.
"""

import pytest

from common import lakehouse


# ─────────────────────────────────────────────
# ZONES
# ─────────────────────────────────────────────

def test_every_zone_has_a_default_prefix():
    assert set(lakehouse.ZONES) == set(lakehouse.DEFAULT_ZONE_PREFIXES)


@pytest.mark.parametrize("zone,expected", [
    ("landing", "landing/"),
    ("bronze", "bronze/"),
    ("silver", "silver/"),
    ("gold", "gold/"),
    ("quarantine", "quarantine/"),
    ("quality", "quality/"),
])
def test_zone_prefix_defaults(zone, expected):
    assert lakehouse.zone_prefix({}, zone) == expected


def test_zone_prefix_normalises_a_sloppy_override():
    assert lakehouse.zone_prefix({"GOLD_PREFIX": "/curated/tables"}, "gold") == "curated/tables/"


def test_zone_prefix_rejects_an_unknown_zone():
    with pytest.raises(ValueError, match="Unknown zone"):
        lakehouse.zone_prefix({}, "platinum")


def test_gold_zone_still_answers_to_the_pre_medallion_key():
    assert lakehouse.zone_prefix({"CURATED_PREFIX": "curated/"}, "gold") == "curated/"


def test_an_explicit_zone_key_beats_the_legacy_one():
    config = {"GOLD_PREFIX": "gold/", "CURATED_PREFIX": "curated/"}

    assert lakehouse.zone_prefix(config, "gold") == "gold/"


# ─────────────────────────────────────────────
# DATASETS
# ─────────────────────────────────────────────

def test_dataset_prefix_nests_under_its_zone():
    assert lakehouse.dataset_prefix({}, "bronze/events") == "bronze/events/"
    assert lakehouse.dataset_prefix({}, "gold/orders") == "gold/orders/"


def test_dataset_prefix_follows_a_moved_zone():
    assert lakehouse.dataset_prefix({"BRONZE_PREFIX": "raw-zone/"}, "bronze/events") == "raw-zone/events/"


def test_dataset_prefix_honours_the_derived_key():
    config = {"BRONZE_EVENTS_PREFIX": "incoming/events/"}

    assert lakehouse.dataset_prefix(config, "bronze/events") == "incoming/events/"


@pytest.mark.parametrize("legacy_key,dataset,expected", [
    ("RAW_PREFIX", "bronze/events", "raw/"),
    ("PROCESSED_PREFIX", "silver/events", "processed/"),
    ("REJECTED_PREFIX", "quarantine/events", "rejected/"),
])
def test_pre_medallion_dataset_keys_still_win(legacy_key, dataset, expected):
    """An existing deployment must keep writing exactly where it wrote before."""
    assert lakehouse.dataset_prefix({legacy_key: expected}, dataset) == expected


def test_an_empty_prefix_means_the_bucket_root():
    """`REJECTED_PREFIX: ""` is the documented way to drop rejected records."""
    assert lakehouse.dataset_prefix({"REJECTED_PREFIX": ""}, "quarantine/events") == ""


# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────

def test_dataset_path_builds_an_s3_uri():
    assert lakehouse.dataset_path({"OUTPUT_BUCKET": "lake"}, "silver/events") == "s3://lake/silver/events/"


def test_dataset_path_passes_an_explicit_uri_through():
    assert lakehouse.dataset_path({}, "s3://other/data") == "s3://other/data/"


def test_dataset_path_honours_a_full_path_override():
    config = {"OUTPUT_BUCKET": "lake", "PROCESSED_S3_PATH": "s3://elsewhere/fact/"}

    assert lakehouse.dataset_path(config, "silver/events") == "s3://elsewhere/fact/"


def test_build_paths_covers_every_zone_and_the_aliases():
    paths = lakehouse.build_paths({"OUTPUT_BUCKET": "lake"})

    assert paths["bucket"] == "lake"
    for zone in lakehouse.ZONES:
        assert paths[zone] == f"s3://lake/{zone}/"
    assert paths["raw"] == paths["bronze_events"] == "s3://lake/bronze/events/"
    assert paths["processed"] == paths["silver_events"] == "s3://lake/silver/events/"
    assert paths["rejected"] == paths["quarantine_events"]
    assert paths["curated"] == paths["gold"]


# ─────────────────────────────────────────────
# GOLD DATASET SELECTION
# ─────────────────────────────────────────────

def test_unset_means_every_gold_table():
    assert lakehouse.resolve_gold_datasets({}) == list(lakehouse.GOLD_DATASETS)


def test_an_empty_list_means_none():
    """The silver job sets this to hand the gold layer over to the gold job."""
    assert lakehouse.resolve_gold_datasets({"GOLD_DATASETS": []}) == []
    assert lakehouse.resolve_gold_datasets({"CURATED_DATASETS": []}) == []


def test_the_new_key_wins_over_the_old_one():
    config = {"GOLD_DATASETS": ["orders"], "CURATED_DATASETS": ["sessions"]}

    assert lakehouse.resolve_gold_datasets(config) == ["orders"]


def test_a_typo_in_a_dataset_name_fails_loudly():
    with pytest.raises(ValueError, match="Unknown gold dataset"):
        lakehouse.resolve_gold_datasets({"GOLD_DATASETS": ["sessons"]})


def test_every_gold_dataset_declares_its_partitioning():
    """`None` means unpartitioned — a missing entry would silently partition by nothing."""
    for name, partitions in lakehouse.GOLD_DATASETS.items():
        assert partitions is None or isinstance(partitions, list), name
