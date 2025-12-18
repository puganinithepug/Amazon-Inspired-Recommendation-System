#Dataset-specific quality checks for project's real CSVs

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import re
import numpy as np

DATA_DIR = Path("data")      # use "/app/data" if running tests inside Docker image
RATINGS_P = DATA_DIR / "explicit_ratings_from_kafka.csv"

def test_rating_distribution_and_variance():
    df = pd.read_csv(RATINGS_P)
    ratings = pd.to_numeric(df['rating'], errors='coerce').dropna()

    mean = ratings.mean()
    var = ratings.var()

    assert 2.5 <= mean <= 4.5, f"Global mean {mean:.2f} outside realistic range (2.5–4.5)"
    assert var > 0.1, f"Rating variance too low ({var:.3f}) — ratings too uniform"

def test_movie_popularity_gini():
    df = pd.read_csv(RATINGS_P)
    counts = df.groupby('movie_id')['rating'].count().values

    def gini(x):
        x = np.sort(np.array(x))
        n = len(x)
        return (2 * np.sum(np.arange(1, n+1) * x) / (n * np.sum(x))) - (n + 1) / n

    g = gini(counts)
    print(f"Gini coefficient of movie rating distribution: {g:.3f}")

    assert 0.5 <= g <= 0.85, f"Gini {g:.3f} out of expected range (0.5–0.85)"


def test_outlier_movies_fraction():
    df = pd.read_csv(RATINGS_P)
    movie_means = df.groupby('movie_id')['rating'].mean()
    mu, sigma = movie_means.mean(), movie_means.std()

    outliers = movie_means[movie_means > mu + 3 * sigma]
    outlier_fraction = len(outliers) / len(movie_means)

    assert outlier_fraction < 0.02, (
        f"Too many movie outliers ({outlier_fraction:.2%}) — possible data corruption"
    )

def test_recency_of_data():
    df = pd.read_csv(RATINGS_P)
    ts = pd.to_datetime(df['timestamp'], errors='coerce').dropna()
    assert len(ts) > 0, "No parseable timestamps found"

    cutoff = ts.max() - timedelta(days=60)
    recent_fraction = (ts >= cutoff).mean()

    assert recent_fraction >= 0.5, (
        f"Only {recent_fraction:.1%} of ratings are from last 60 days — data may be stale"
    )
