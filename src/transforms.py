"""Reusable DataFrame transformations for the Airbnb analytics pipeline.

These functions are pure: each takes and returns a DataFrame with no I/O, so
they can be unit tested against a small local SparkSession. The techniques here
mirror the ZTM Spark course: regexp price cleaning, filters, aggregations,
inner and left-anti joins, window functions, and Python UDFs.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .config import PipelineConfig

# Polarity word banks for the sentiment UDF, kept small and deterministic.
POSITIVE_WORDS: frozenset[str] = frozenset(
    {
        "good",
        "great",
        "excellent",
        "amazing",
        "fantastic",
        "wonderful",
        "pleasant",
        "lovely",
        "nice",
        "clean",
        "comfortable",
        "spotless",
        "spacious",
    }
)
NEGATIVE_WORDS: frozenset[str] = frozenset(
    {
        "bad",
        "terrible",
        "awful",
        "horrible",
        "disappointing",
        "poor",
        "hate",
        "unpleasant",
        "dirty",
        "noisy",
        "cramped",
    }
)


def clean_listings(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """Parse the price string and drop listings outside a plausible range.

    The raw ``price`` column arrives as a currency string such as ``$1,200.00``.
    This strips the symbol and separators, casts to double, and filters to the
    configured price window, dropping nulls introduced by unparseable values.

    Args:
        df: Raw listings DataFrame matching ``LISTINGS_SCHEMA``.
        config: Pipeline configuration holding price bounds.

    Returns:
        A cleaned listings DataFrame with a numeric ``price_num`` column.
    """
    with_price = df.withColumn(
        "price_num", F.regexp_replace(F.col("price"), r"[$,]", "").cast("double")
    )
    return with_price.filter(
        F.col("price_num").isNotNull()
        & (F.col("price_num") >= config.min_price)
        & (F.col("price_num") <= config.max_price)
    )


def _price_category(config: PipelineConfig) -> Column:
    """Column expression bucketing price into Budget, Mid-range, and Luxury."""
    return (
        F.when(F.col("price_num") < config.budget_ceiling, F.lit("Budget"))
        .when(F.col("price_num") < config.midrange_ceiling, F.lit("Mid-range"))
        .otherwise(F.lit("Luxury"))
    )


def add_price_category(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """Add a ``price_category`` band to cleaned listings.

    Args:
        df: Cleaned listings DataFrame with ``price_num``.
        config: Pipeline configuration holding band ceilings.

    Returns:
        The listings DataFrame with a ``price_category`` column.
    """
    return df.withColumn("price_category", _price_category(config))


def price_by_neighbourhood(df: DataFrame) -> DataFrame:
    """Average and median-style price stats per neighbourhood.

    Ranks neighbourhoods by average price with a window function so the busiest
    and priciest areas can be sliced without a second pass.

    Args:
        df: Cleaned listings DataFrame with ``price_num``.

    Returns:
        Per-neighbourhood price metrics ordered by average price descending.
    """
    grouped = df.groupBy("neighbourhood_cleansed").agg(
        F.count("*").alias("listing_count"),
        F.round(F.avg("price_num"), 2).alias("avg_price"),
        F.round(F.min("price_num"), 2).alias("min_price"),
        F.round(F.max("price_num"), 2).alias("max_price"),
        F.round(F.avg("review_scores_rating"), 2).alias("avg_rating"),
    )
    rank_window = Window.orderBy(F.col("avg_price").desc())
    return grouped.withColumn("price_rank", F.rank().over(rank_window)).orderBy(
        F.col("avg_price").desc()
    )


def reviews_per_listing(listings: DataFrame, reviews: DataFrame, top_n: int) -> DataFrame:
    """Count reviews per listing via an inner join, keeping the busiest listings.

    Args:
        listings: Cleaned listings DataFrame.
        reviews: Raw reviews DataFrame.
        top_n: Number of top listings to return.

    Returns:
        The most-reviewed listings with their review counts.
    """
    joined = listings.join(reviews, listings.id == reviews.listing_id, how="inner")
    return (
        joined.groupBy(listings.id, listings.name)
        .agg(F.count(reviews.id).alias("num_reviews"))
        .orderBy(F.col("num_reviews").desc())
        .limit(top_n)
    )


def listings_without_reviews(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Find listings that have never been reviewed using a left-anti join.

    Args:
        listings: Cleaned listings DataFrame.
        reviews: Raw reviews DataFrame.

    Returns:
        Listings with no matching review, one row per listing.
    """
    return listings.join(
        reviews, listings.id == reviews.listing_id, how="left_anti"
    ).select("id", "name", "neighbourhood_cleansed", "price_num")


def price_category_summary(df: DataFrame) -> DataFrame:
    """Count listings and average rating within each price band.

    Args:
        df: Listings DataFrame that already carries ``price_category``.

    Returns:
        Per-band counts and average rating, ordered by count descending.
    """
    return (
        df.groupBy("price_category")
        .agg(
            F.count("*").alias("listing_count"),
            F.round(F.avg("price_num"), 2).alias("avg_price"),
            F.round(F.avg("review_scores_rating"), 2).alias("avg_rating"),
        )
        .orderBy(F.col("listing_count").desc())
    )


def _sentiment_score(comment: str | None) -> int:
    """Score a review by positive minus negative word hits.

    Args:
        comment: Raw review text, possibly None.

    Returns:
        An integer sentiment score; higher is more positive.
    """
    if not comment:
        return 0
    tokens = comment.lower().replace(".", " ").replace(",", " ").split()
    score = 0
    for token in tokens:
        if token in POSITIVE_WORDS:
            score += 1
        elif token in NEGATIVE_WORDS:
            score -= 1
    return score


# Registered once at import so the same UDF instance is reused across the job.
sentiment_udf = F.udf(_sentiment_score, StringType())


def sentiment_by_neighbourhood(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Average review sentiment per neighbourhood via a UDF and inner join.

    Args:
        listings: Cleaned listings DataFrame.
        reviews: Raw reviews DataFrame.

    Returns:
        Per-neighbourhood average sentiment ordered from most to least positive.
    """
    scored = reviews.withColumn("sentiment_score", sentiment_udf(F.col("comments")).cast("double"))
    joined = listings.join(scored, listings.id == scored.listing_id, how="inner")
    return (
        joined.groupBy("neighbourhood_cleansed")
        .agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("sentiment_score"), 3).alias("avg_sentiment"),
        )
        .orderBy(F.col("avg_sentiment").desc())
    )
