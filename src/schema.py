"""Explicit Spark schemas for the Airbnb analytics pipeline.

The columns mirror the Inside Airbnb ``listings`` and ``reviews`` exports used
by the ZTM course, reduced to the fields the pipeline actually consumes.
Declaring schemas up front avoids inference scans and pins column types.

Note that ``price`` is intentionally a string such as ``$1,200.00``; the
cleaning stage strips the currency symbol and thousands separators, matching the
course exercise that teaches ``regexp_replace``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Subset of the Inside Airbnb listings export consumed by the pipeline.
LISTINGS_SCHEMA: StructType = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("host_id", LongType(), nullable=True),
        StructField("host_name", StringType(), nullable=True),
        StructField("neighbourhood_cleansed", StringType(), nullable=True),
        StructField("room_type", StringType(), nullable=True),
        StructField("property_type", StringType(), nullable=True),
        StructField("price", StringType(), nullable=True),
        StructField("minimum_nights", IntegerType(), nullable=True),
        StructField("number_of_reviews", IntegerType(), nullable=True),
        StructField("reviews_per_month", DoubleType(), nullable=True),
        StructField("review_scores_rating", DoubleType(), nullable=True),
        StructField("review_scores_location", DoubleType(), nullable=True),
        StructField("first_review", DateType(), nullable=True),
    ]
)

# Subset of the Inside Airbnb reviews export consumed by the pipeline.
REVIEWS_SCHEMA: StructType = StructType(
    [
        StructField("listing_id", LongType(), nullable=False),
        StructField("id", LongType(), nullable=False),
        StructField("date", DateType(), nullable=True),
        StructField("reviewer_id", LongType(), nullable=True),
        StructField("reviewer_name", StringType(), nullable=True),
        StructField("comments", StringType(), nullable=True),
    ]
)
