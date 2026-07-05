"""Unit tests for the pure DataFrame transformations."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from src.config import PipelineConfig
from src.schema import LISTINGS_SCHEMA, REVIEWS_SCHEMA
from src.transforms import (
    add_price_category,
    clean_listings,
    listings_without_reviews,
    price_category_summary,
    reviews_per_listing,
    sentiment_by_neighbourhood,
)


def _listing_rows() -> list[tuple]:
    """Five listings: four parseable, one with an unparseable price."""
    return [
        (1, "Budget room in Camden", 1001, "Sam", "Camden", "Private room", "Private room in home", "$40.00", 2, 12, 1.5, 4.6, 4.7, date(2019, 3, 1)),
        (2, "Mid flat in Hackney", 1002, "Alex", "Hackney", "Entire home/apt", "Entire rental unit", "$120.00", 3, 8, 1.1, 4.4, 4.5, date(2020, 6, 1)),
        (3, "Luxury loft in Westminster", 1003, "Priya", "Westminster", "Entire home/apt", "Entire condo", "$1,250.00", 5, 3, 0.4, 4.9, 5.0, date(2018, 1, 1)),
        (4, "Central room in Islington", 1004, "Marco", "Islington", "Private room", "Private room in home", "$75.00", 1, 0, None, None, None, None),
        (5, "Broken price listing", 1005, "Ola", "Brent", "Shared room", "Shared room", "N/A", 1, 1, 0.2, 4.0, 4.0, date(2021, 1, 1)),
    ]


def _review_rows() -> list[tuple]:
    """Reviews for listings 1 and 2 only; listings 3 and 4 have none."""
    return [
        (1, 100, date(2021, 1, 1), 5001, "Jamie", "The place was great and clean."),
        (1, 101, date(2021, 2, 1), 5002, "Noor", "Lovely and spotless, would return."),
        (2, 102, date(2021, 3, 1), 5003, "Diego", "It felt a little noisy and dirty."),
    ]


def test_clean_listings_parses_price_and_drops_unparseable(spark: SparkSession) -> None:
    """Price string is stripped to a number and the N/A row is dropped."""
    config = PipelineConfig()
    df = spark.createDataFrame(_listing_rows(), schema=LISTINGS_SCHEMA)

    cleaned = clean_listings(df, config)

    # The unparseable "N/A" price becomes null and is filtered out.
    assert cleaned.count() == 4
    prices = {row["id"]: row["price_num"] for row in cleaned.collect()}
    assert prices[1] == 40.0
    assert prices[3] == 1250.0  # Thousands separator handled.
    assert 5 not in prices


def test_add_price_category_bands(spark: SparkSession) -> None:
    """Price bands split into Budget, Mid-range, and Luxury by ceiling."""
    config = PipelineConfig()
    df = spark.createDataFrame(_listing_rows(), schema=LISTINGS_SCHEMA)
    categorized = add_price_category(clean_listings(df, config), config)

    bands = {row["id"]: row["price_category"] for row in categorized.collect()}
    assert bands[1] == "Budget"  # 40 < 50
    assert bands[2] == "Mid-range"  # 50 <= 120 < 150
    assert bands[3] == "Luxury"  # 1250 >= 150

    summary = {row["price_category"]: row["listing_count"] for row in price_category_summary(categorized).collect()}
    assert summary["Budget"] == 1
    assert summary["Mid-range"] == 2  # Listings 2 and 4 (75 is mid-range).
    assert summary["Luxury"] == 1


def test_reviews_per_listing_counts_and_orders(spark: SparkSession) -> None:
    """Listing 1 has two reviews and outranks listing 2 with one."""
    config = PipelineConfig()
    listings = clean_listings(spark.createDataFrame(_listing_rows(), schema=LISTINGS_SCHEMA), config)
    reviews = spark.createDataFrame(_review_rows(), schema=REVIEWS_SCHEMA)

    result = reviews_per_listing(listings, reviews, top_n=10).collect()

    assert result[0]["id"] == 1
    assert result[0]["num_reviews"] == 2
    assert {row["id"] for row in result} == {1, 2}


def test_listings_without_reviews_uses_left_anti(spark: SparkSession) -> None:
    """Left-anti join surfaces exactly the unreviewed listings."""
    config = PipelineConfig()
    listings = clean_listings(spark.createDataFrame(_listing_rows(), schema=LISTINGS_SCHEMA), config)
    reviews = spark.createDataFrame(_review_rows(), schema=REVIEWS_SCHEMA)

    unreviewed_ids = {row["id"] for row in listings_without_reviews(listings, reviews).collect()}

    # Listings 3 and 4 have no reviews; listing 5 was dropped as unparseable.
    assert unreviewed_ids == {3, 4}


def test_sentiment_by_neighbourhood_scores_polarity(spark: SparkSession) -> None:
    """Camden reviews are positive while Hackney's lone review is negative."""
    config = PipelineConfig()
    listings = clean_listings(spark.createDataFrame(_listing_rows(), schema=LISTINGS_SCHEMA), config)
    reviews = spark.createDataFrame(_review_rows(), schema=REVIEWS_SCHEMA)

    scores = {row["neighbourhood_cleansed"]: row["avg_sentiment"] for row in sentiment_by_neighbourhood(listings, reviews).collect()}

    assert scores["Camden"] > 0  # "great", "clean", "lovely", "spotless".
    assert scores["Hackney"] < 0  # "noisy", "dirty".
