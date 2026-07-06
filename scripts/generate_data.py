"""Synthetic Inside-Airbnb-shaped data generator.

This is an OPTIONAL offline fallback only. The default, documented data path is
the real Inside Airbnb London open dataset fetched by ``scripts/download_data.sh``.
Use this generator when you want to run the pipeline with no network access; it
writes gzipped ``listings.csv.gz`` and ``reviews.csv.gz`` to ``data/raw`` (the
same paths the pipeline reads by default), reproducing the real column shapes and
quirks, most importantly the price string format ``$1,234.00`` that the pipeline
must clean.

Run with the project venv:

    python scripts/generate_data.py --listings 8000 --reviews 120000
"""

from __future__ import annotations

import argparse
import csv
import gzip
import random
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# London boroughs, matching the neighbourhood_cleansed field of the real export.
NEIGHBOURHOODS: list[str] = [
    "Westminster",
    "Camden",
    "Hackney",
    "Tower Hamlets",
    "Islington",
    "Southwark",
    "Lambeth",
    "Kensington and Chelsea",
    "Wandsworth",
    "Newham",
    "Brent",
    "Ealing",
    "Hammersmith and Fulham",
    "Greenwich",
]

ROOM_TYPES: list[str] = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]
ROOM_WEIGHTS: list[float] = [0.55, 0.38, 0.05, 0.02]

PROPERTY_TYPES: list[str] = [
    "Entire rental unit",
    "Private room in rental unit",
    "Private room in home",
    "Entire condo",
    "Entire home",
    "Room in boutique hotel",
    "Entire serviced apartment",
]

# Word banks tuned so the sentiment UDF has signal to find.
POSITIVE_WORDS: list[str] = [
    "great",
    "amazing",
    "lovely",
    "excellent",
    "wonderful",
    "clean",
    "comfortable",
    "spotless",
    "spacious",
]
NEGATIVE_WORDS: list[str] = [
    "dirty",
    "noisy",
    "disappointing",
    "poor",
    "terrible",
    "cramped",
    "unpleasant",
]
NEUTRAL_PHRASES: list[str] = [
    "The location was central and easy to reach.",
    "Check in was straightforward and the host responded quickly.",
    "The flat matched the photos in the listing.",
    "We stayed three nights and would consider returning.",
    "Transport links nearby made getting around simple.",
]


def _price_string(rng: random.Random, room_type: str) -> str:
    """Format a nightly price the way Inside Airbnb exports it.

    Entire homes skew pricier than private and shared rooms. The value is
    rendered as a currency string with thousands separators so the pipeline
    must strip ``$`` and ``,`` before casting.
    """
    if room_type == "Entire home/apt":
        base = rng.lognormvariate(5.0, 0.5)
    elif room_type == "Private room":
        base = rng.lognormvariate(4.3, 0.4)
    else:
        base = rng.lognormvariate(3.9, 0.4)
    value = round(min(max(base, 15.0), 6000.0), 2)
    return f"${value:,.2f}"


def _review_comment(rng: random.Random) -> str:
    """Compose a review comment blending polarity words and neutral filler."""
    parts: list[str] = []
    for _ in range(rng.randint(1, 3)):
        roll = rng.random()
        if roll < 0.55:
            parts.append(f"The place was {rng.choice(POSITIVE_WORDS)}.")
        elif roll < 0.75:
            parts.append(f"It felt a little {rng.choice(NEGATIVE_WORDS)} at times.")
        else:
            parts.append(rng.choice(NEUTRAL_PHRASES))
    return " ".join(parts)


def generate_listings(count: int, rng: random.Random) -> list[dict[str, object]]:
    """Build synthetic listing rows.

    Args:
        count: Number of listings to create.
        rng: Seeded random generator.

    Returns:
        A list of listing dictionaries keyed by column name.
    """
    listings: list[dict[str, object]] = []
    for listing_id in range(1, count + 1):
        room_type = rng.choices(ROOM_TYPES, weights=ROOM_WEIGHTS, k=1)[0]
        num_reviews = int(rng.lognormvariate(2.0, 1.2))
        has_reviews = num_reviews > 0
        first_review = (
            date(2015, 1, 1) + timedelta(days=rng.randint(0, 3200)) if has_reviews else None
        )
        listings.append(
            {
                "id": listing_id,
                "name": f"{rng.choice(['Cosy', 'Bright', 'Modern', 'Central', 'Quiet'])} "
                f"{room_type.split('/')[0].lower()} in {rng.choice(NEIGHBOURHOODS)}",
                "host_id": rng.randint(1000, 1000 + count // 3),
                "host_name": rng.choice(
                    ["Alex", "Sam", "Priya", "Marco", "Ola", "Chen", "Fatima", "Liam"]
                ),
                "neighbourhood_cleansed": rng.choice(NEIGHBOURHOODS),
                "room_type": room_type,
                "property_type": rng.choice(PROPERTY_TYPES),
                "price": _price_string(rng, room_type),
                "minimum_nights": rng.choice([1, 1, 2, 2, 3, 5, 7, 30]),
                "number_of_reviews": num_reviews,
                "reviews_per_month": round(rng.uniform(0.0, 6.0), 2) if has_reviews else None,
                "review_scores_rating": round(rng.uniform(3.5, 5.0), 2) if has_reviews else None,
                "review_scores_location": round(rng.uniform(3.8, 5.0), 2) if has_reviews else None,
                "first_review": first_review.isoformat() if first_review else "",
            }
        )
    return listings


def generate_reviews(
    listings: list[dict[str, object]], target: int, rng: random.Random
) -> list[dict[str, object]]:
    """Build synthetic review rows consistent with each listing's count.

    Args:
        listings: Previously generated listings.
        target: Approximate total number of reviews to emit.
        rng: Seeded random generator.

    Returns:
        A list of review dictionaries keyed by column name.
    """
    reviews: list[dict[str, object]] = []
    review_id = 1
    weighted = [row for row in listings if row["number_of_reviews"]]
    if not weighted:
        return reviews

    while len(reviews) < target:
        listing = rng.choice(weighted)
        review_date = date(2016, 1, 1) + timedelta(days=rng.randint(0, 3100))
        reviews.append(
            {
                "listing_id": listing["id"],
                "id": review_id,
                "date": review_date.isoformat(),
                "reviewer_id": rng.randint(50_000, 999_999),
                "reviewer_name": rng.choice(
                    ["Jamie", "Noor", "Diego", "Yuki", "Emma", "Tomas", "Aisha", "Ben"]
                ),
                "comments": _review_comment(rng),
            }
        )
        review_id += 1
    return reviews


def _write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    """Write dictionaries to a gzipped CSV with a fixed column order."""
    with gzip.open(path, "wt", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)


def generate(listings_count: int, reviews_count: int, seed: int) -> tuple[Path, Path]:
    """Generate both datasets and write them to ``data/raw``.

    Args:
        listings_count: Number of listings to generate.
        reviews_count: Approximate number of reviews to generate.
        seed: Random seed for reproducible output.

    Returns:
        Paths to the written listings and reviews CSV files.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    listings = generate_listings(listings_count, rng)
    reviews = generate_reviews(listings, reviews_count, rng)

    listings_path = RAW_DIR / "listings.csv.gz"
    reviews_path = RAW_DIR / "reviews.csv.gz"

    _write_csv(
        listings_path,
        listings,
        [
            "id",
            "name",
            "host_id",
            "host_name",
            "neighbourhood_cleansed",
            "room_type",
            "property_type",
            "price",
            "minimum_nights",
            "number_of_reviews",
            "reviews_per_month",
            "review_scores_rating",
            "review_scores_location",
            "first_review",
        ],
    )
    _write_csv(
        reviews_path,
        reviews,
        ["listing_id", "id", "date", "reviewer_id", "reviewer_name", "comments"],
    )
    return listings_path, reviews_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Airbnb data.")
    parser.add_argument("--listings", type=int, default=8_000, help="Number of listings.")
    parser.add_argument("--reviews", type=int, default=120_000, help="Approximate reviews.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for data generation."""
    args = _parse_args()
    listings_path, reviews_path = generate(args.listings, args.reviews, args.seed)
    print(f"Wrote {args.listings} listings to {listings_path}")
    print(f"Wrote about {args.reviews} reviews to {reviews_path}")


if __name__ == "__main__":
    main()
