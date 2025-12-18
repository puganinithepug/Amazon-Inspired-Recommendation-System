import argparse
import pickle
from pathlib import Path

import pandas as pd
from surprise import Reader, Dataset
from surprise.model_selection import train_test_split

BASE_DIR = Path(__file__).resolve().parent.parent

def prepare_surprise_data(csv_path, test_size, random_state,
                           trainset_output_path=None, testset_output_path=None):
    """
    Loads data from a CSV, splits it into Surprise trainset/testset,
    and optionally saves them to pickle files.

    Args:
        csv_path (str): Path to the input CSV file containing user, item, rating.
        test_size (float): Proportion of the dataset to include in the test split.
        random_state (int): Seed for random number generator for reproducibility.
        trainset_output_path (str, optional): Path to save the trainset pickle.
        testset_output_path (str, optional): Path to save the testset pickle.

    Returns:
        tuple: (trainset, testset) objects from Surprise.
    """
    print(f"INFO: Loading data from {csv_path} for processing...")
    ratings = pd.read_csv(csv_path)

    min_rating = ratings['rating'].min()
    max_rating = ratings['rating'].max()
    print(f"INFO: Detected rating scale: ({min_rating}, {max_rating})")

    reader = Reader(rating_scale=(min_rating, max_rating))
    data = Dataset.load_from_df(ratings[['user_id', 'movie_id', 'rating']], reader)

    trainset, testset = train_test_split(data, test_size=test_size, random_state=random_state)

    print(f"INFO: Data split complete. Trainset size: {trainset.n_ratings}, Testset size: {len(testset)}")

    if trainset_output_path:
        with open(trainset_output_path, 'wb') as f:
            pickle.dump(trainset, f)
        print(f"INFO: Trainset saved to {trainset_output_path}")

    if testset_output_path:
        with open(testset_output_path, 'wb') as f:
            pickle.dump(testset, f)
        print(f"INFO: Testset saved to {testset_output_path}")

    return trainset, testset, ratings


def main():
    parser = argparse.ArgumentParser(description="Split ratings CSV into Surprise train/test sets.")
    parser.add_argument("--csv-path", default=str(BASE_DIR / "data" / "explicit_ratings_from_kafka.csv"),
                        help="Path to ratings CSV.")
    parser.add_argument("--trainset-output", default=str(BASE_DIR / "data" / "surprise_trainset.pkl"),
                        help="Output path for Surprise trainset pickle.")
    parser.add_argument("--testset-output", default=str(BASE_DIR / "data" / "surprise_testset.pkl"),
                        help="Output path for Surprise testset pickle.")
    parser.add_argument("--test-size", type=float, default=0.2,
                        help="Fraction of ratings for the test split.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed.")
    args = parser.parse_args()

    train, test, _ = prepare_surprise_data(
        csv_path=args.csv_path,
        test_size=args.test_size,
        random_state=args.random_state,
        trainset_output_path=args.trainset_output,
        testset_output_path=args.testset_output,
    )
    print(f"Trainset size (number of ratings): {train.n_ratings}")
    print(f"Trainset number of users: {train.n_users}")
    print(f"Trainset number of items: {train.n_items}")
    print(f"Testset size (number of ratings): {len(test)}")


if __name__ == '__main__':
    main()
