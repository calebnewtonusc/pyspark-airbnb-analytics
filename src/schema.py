"""Column selection and target types for the Airbnb analytics pipeline.

The real Inside Airbnb ``listings`` export is a wide CSV with roughly 75 columns
in a provider-defined order, and its text fields contain embedded newlines,
commas, and quotes. Applying a positional ``StructType`` to such a file would
misalign columns, so instead the pipeline reads every column as a string (header
on, no inference scan) and then projects and casts only the fields it consumes,
selecting by name. The maps below declare that name-to-type projection.

Note that ``price`` is intentionally kept as a string such as ``$1,200.00``; the
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

# Columns the pipeline consumes from the Inside Airbnb listings export, mapped to
# the Spark type each should be cast to after being read as a string. ``price``
# stays a string on purpose so the cleaning stage can parse the currency format.
LISTINGS_COLUMNS: dict[str, str] = {
    "id": "long",
    "name": "string",
    "host_id": "long",
    "host_name": "string",
    "neighbourhood_cleansed": "string",
    "room_type": "string",
    "property_type": "string",
    "price": "string",
    "minimum_nights": "int",
    "number_of_reviews": "int",
    "reviews_per_month": "double",
    "review_scores_rating": "double",
    "review_scores_location": "double",
    "first_review": "date",
}

# Columns the pipeline consumes from the Inside Airbnb reviews export.
REVIEWS_COLUMNS: dict[str, str] = {
    "listing_id": "long",
    "id": "long",
    "date": "date",
    "reviewer_id": "long",
    "reviewer_name": "string",
    "comments": "string",
}

# Map the short type names above to Spark SQL types for building test schemas.
_TYPE_MAP = {
    "long": LongType(),
    "int": IntegerType(),
    "double": DoubleType(),
    "date": DateType(),
    "string": StringType(),
}


def _struct(columns: dict[str, str]) -> StructType:
    """Build a nullable StructType from a name-to-type-name mapping."""
    return StructType(
        [StructField(name, _TYPE_MAP[type_name], nullable=True) for name, type_name in columns.items()]
    )


# Concrete StructTypes for the projected columns, used by the unit tests to
# construct small in-memory DataFrames with the exact types the pipeline emits.
LISTINGS_SCHEMA: StructType = _struct(LISTINGS_COLUMNS)
REVIEWS_SCHEMA: StructType = _struct(REVIEWS_COLUMNS)
