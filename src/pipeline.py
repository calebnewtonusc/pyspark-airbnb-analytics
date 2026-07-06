"""End-to-end PySpark analytics pipeline over Airbnb listings and reviews.

Reads raw listings and reviews CSV, cleans and enriches listings, then builds
gold-layer analytics tables using the DataFrame API, window functions, UDFs, and
a Spark SQL query over temporary views. Gold tables are written as parquet,
partitioned by price band where it makes sense. Run with:

    python -m src.pipeline

Environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are pinned to the
active interpreter so Spark workers match the driver.
"""

from __future__ import annotations

import os
import sys

# Pin the worker interpreter to the driver before Spark is imported so a
# mismatched Python on the workers cannot break serialization.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from .config import PipelineConfig, load_config  # noqa: E402
from .schema import LISTINGS_COLUMNS, REVIEWS_COLUMNS  # noqa: E402
from .transforms import (  # noqa: E402
    add_price_category,
    clean_listings,
    listings_without_reviews,
    price_by_neighbourhood,
    price_category_summary,
    reviews_per_listing,
    sentiment_by_neighbourhood,
)


def build_spark(config: PipelineConfig) -> SparkSession:
    """Create a configured local SparkSession.

    Args:
        config: Pipeline configuration.

    Returns:
        An active SparkSession.
    """
    builder = (
        SparkSession.builder.appName(config.app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
    )
    for key, value in config.spark_extra.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _read_csv(spark: SparkSession, path: str, columns: dict[str, str]) -> DataFrame:
    """Read a real Inside Airbnb (gzipped) CSV and project the needed columns.

    The real exports are wide CSVs (the listings file has ~75 columns) whose text
    fields carry embedded newlines, commas, and quotes. Reading them with a
    positional ``StructType`` would misalign columns, so every field is read as a
    string with ``header`` on and no inference scan, using the multiLine, quote,
    and escape options the malformed-record-tolerant reader needs. The wanted
    columns are then selected by name and cast to their target types. Spark reads
    the ``.gz`` files transparently by file extension.

    Args:
        spark: Active SparkSession.
        path: Path to the (optionally gzipped) CSV file.
        columns: Mapping of column name to target Spark type name.

    Returns:
        A DataFrame with exactly ``columns`` projected and cast.
    """
    raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    projected = [F.col(name).cast(type_name).alias(name) for name, type_name in columns.items()]
    return raw.select(*projected)


def read_listings(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read the raw listings CSV and project the columns the pipeline consumes."""
    return _read_csv(spark, config.listings_path, LISTINGS_COLUMNS)


def read_reviews(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read the raw reviews CSV and project the columns the pipeline consumes."""
    return _read_csv(spark, config.reviews_path, REVIEWS_COLUMNS)


def top_comment_length_sql(
    spark: SparkSession, listings: DataFrame, reviews: DataFrame, min_reviews: int
) -> DataFrame:
    """Rank listings by average review comment length using Spark SQL.

    Mirrors the course exercise that rewrites a DataFrame aggregation as a SQL
    query over temporary views, filtering to listings with enough reviews.

    Args:
        spark: Active SparkSession.
        listings: Cleaned listings DataFrame.
        reviews: Raw reviews DataFrame.
        min_reviews: Minimum reviews a listing needs to qualify.

    Returns:
        Listings ranked by average comment length descending.
    """
    listings.createOrReplaceTempView("listings")
    reviews.createOrReplaceTempView("reviews")
    return spark.sql(
        f"""
        SELECT
            r.listing_id,
            l.name,
            ROUND(AVG(LENGTH(r.comments)), 1) AS avg_comment_length,
            COUNT(r.id) AS reviews_count
        FROM reviews r
        INNER JOIN listings l ON r.listing_id = l.id
        GROUP BY r.listing_id, l.name
        HAVING COUNT(r.id) >= {min_reviews}
        ORDER BY avg_comment_length DESC
        """
    )


def write_gold(df: DataFrame, config: PipelineConfig, name: str, partition: str | None) -> None:
    """Write a gold table to parquet, optionally partitioned.

    Args:
        df: Table to persist.
        config: Pipeline configuration.
        name: Subdirectory name under the gold path.
        partition: Optional partition column, or None for an unpartitioned write.
    """
    path = os.path.join(config.gold_path, name)
    writer = df.repartition(config.output_partitions).write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(path)


def run(config: PipelineConfig, spark: SparkSession | None = None) -> dict[str, int]:
    """Execute the full analytics pipeline and return per-table row counts.

    Args:
        config: Pipeline configuration.
        spark: Optional existing SparkSession, mainly for testing.

    Returns:
        Mapping of gold table name to written row count.
    """
    owns_spark = spark is None
    spark = spark or build_spark(config)

    try:
        raw_listings = read_listings(spark, config)
        reviews = read_reviews(spark, config)

        listings = add_price_category(clean_listings(raw_listings, config), config)

        # Cache the cleaned listings since several tables read from it.
        listings.cache()
        listing_total = listings.count()

        by_neighbourhood = price_by_neighbourhood(listings)
        top_reviewed = reviews_per_listing(listings, reviews, config.top_n)
        no_reviews = listings_without_reviews(listings, reviews)
        band_summary = price_category_summary(listings)
        sentiment = sentiment_by_neighbourhood(listings, reviews)
        comment_length = top_comment_length_sql(
            spark, listings, reviews, config.min_reviews_for_ranking
        ).limit(config.top_n)

        # The listings fact is partitioned by price band; aggregates are small.
        write_gold(listings, config, "listings_clean", partition="price_category")
        write_gold(by_neighbourhood, config, "price_by_neighbourhood", partition=None)
        write_gold(top_reviewed, config, "top_reviewed_listings", partition=None)
        write_gold(no_reviews, config, "listings_without_reviews", partition=None)
        write_gold(band_summary, config, "price_category_summary", partition=None)
        write_gold(sentiment, config, "sentiment_by_neighbourhood", partition=None)
        write_gold(comment_length, config, "top_comment_length", partition=None)

        counts = {
            "listings_clean": listing_total,
            "price_by_neighbourhood": by_neighbourhood.count(),
            "top_reviewed_listings": top_reviewed.count(),
            "listings_without_reviews": no_reviews.count(),
            "price_category_summary": band_summary.count(),
            "sentiment_by_neighbourhood": sentiment.count(),
            "top_comment_length": comment_length.count(),
        }

        print("Gold tables written to", config.gold_path)
        for table, count in counts.items():
            print(f"  {table}: {count} rows")

        print("\nSample: average price by neighbourhood (top 5)")
        by_neighbourhood.show(5, truncate=False)
        print("Sample: listing count by price band")
        band_summary.show(truncate=False)
        print("Sample: average review sentiment by neighbourhood (top 5)")
        sentiment.show(5, truncate=False)

        listings.unpersist()
        return counts
    finally:
        if owns_spark:
            spark.stop()


def main() -> None:
    """CLI entrypoint."""
    config = load_config()
    run(config)


if __name__ == "__main__":
    main()
