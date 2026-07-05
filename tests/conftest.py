"""Shared pytest fixtures for the Airbnb analytics test suite."""

from __future__ import annotations

import os
import sys

# Match the Spark worker interpreter to the driver before importing pyspark.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from collections.abc import Iterator  # noqa: E402

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Provide a lightweight local SparkSession for the whole test session."""
    session = (
        SparkSession.builder.appName("airbnb-analytics-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
