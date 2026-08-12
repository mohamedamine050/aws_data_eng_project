"""Pytest configuration and fixtures."""
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Mock awsglue module before importing glue_ecommerce_processing
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()

# Mock getResolvedOptions to return a dict
def mock_get_resolved_options(args, option_names):
    """Mock implementation of getResolvedOptions."""
    return {opt: f"mock_{opt}" for opt in option_names}

sys.modules['awsglue.utils'].getResolvedOptions = mock_get_resolved_options

# Let boto3.client(...) be constructed at import time without real credentials,
# and keep CloudWatch out of the unit tests entirely.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ["METRICS_ENABLED"] = "false"

_SRC = Path(__file__).resolve().parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession for the transformation tests.

    Skips the whole module when PySpark is not installed, so the pure-Python
    suite still runs in a bare environment (`pip install -r requirements-dev.txt`
    without the optional Spark extra).
    """
    pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
    from pyspark.sql import SparkSession

    del pyspark
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ecommerce-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
