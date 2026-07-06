"""Central configuration for the Airbnb analytics pipeline.

Paths, Spark options, and analysis thresholds live here so the pipeline code
stays free of hardcoded literals. Values can be overridden at runtime by loading
a YAML file through :func:`load_config`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

# Repository root resolved relative to this file so the project is portable.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable settings container for the analytics job.

    Attributes:
        listings_path: CSV of Airbnb listings.
        reviews_path: CSV of Airbnb reviews.
        gold_path: Output directory for gold-layer parquet.
        app_name: Spark application name shown in the Spark UI.
        shuffle_partitions: Value for spark.sql.shuffle.partitions.
        output_partitions: Partition count used before the final write.
        min_price: Lower bound for a plausible nightly price.
        max_price: Upper bound for a plausible nightly price.
        min_reviews_for_ranking: Review floor for comment-length ranking.
        top_n: Row cap for "top N" gold tables.
        budget_ceiling: Upper bound of the Budget price band.
        midrange_ceiling: Upper bound of the Mid-range price band.
    """

    listings_path: str = str(PROJECT_ROOT / "data" / "raw" / "listings.csv.gz")
    reviews_path: str = str(PROJECT_ROOT / "data" / "raw" / "reviews.csv.gz")
    gold_path: str = str(PROJECT_ROOT / "data" / "gold")
    app_name: str = "airbnb-analytics"
    shuffle_partitions: int = 8
    output_partitions: int = 4
    min_price: float = 10.0
    max_price: float = 10_000.0
    min_reviews_for_ranking: int = 5
    top_n: int = 20
    budget_ceiling: float = 50.0
    midrange_ceiling: float = 150.0
    spark_extra: dict[str, str] = field(default_factory=dict)


def load_config(yaml_path: str | None = None) -> PipelineConfig:
    """Build a :class:`PipelineConfig`, optionally overlaying a YAML file.

    Args:
        yaml_path: Optional path to a YAML file whose top-level keys override
            the defaults. When omitted, the environment variable
            ``AIRBNB_ETL_CONFIG`` is consulted before falling back to defaults.

    Returns:
        A populated, immutable :class:`PipelineConfig`.
    """
    resolved = yaml_path or os.environ.get("AIRBNB_ETL_CONFIG")
    config = PipelineConfig()
    if not resolved:
        return config

    import yaml  # Imported lazily so PyYAML is optional at runtime.

    with open(resolved, "r", encoding="utf-8") as handle:
        overrides = yaml.safe_load(handle) or {}

    fields = PipelineConfig.__dataclass_fields__
    clean: dict[str, object] = {}
    for key, value in overrides.items():
        if key not in fields:
            continue
        declared = fields[key].type
        if key == "spark_extra":
            clean[key] = value
        elif declared == "float":
            clean[key] = float(value)
        elif declared == "int":
            clean[key] = int(value)
        else:
            clean[key] = value
    return replace(config, **clean)
