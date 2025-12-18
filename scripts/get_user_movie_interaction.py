import pandas as pd
import subprocess
import json

def get_user_info(user_id):
    """Fetch user information using a curl request and return dict."""
    url = f"http://fall2025-comp585.cs.mcgill.ca:8080/user/{user_id}"
    try:
        result = subprocess.run(["curl", "-s", url], capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return {}  # return empty dict on failure

def get_movie_info(movie_id):
    """Fetch movie information using a curl request and return dict."""
    url = f"http://fall2025-comp585.cs.mcgill.ca:8080/movie/{movie_id}"
    try:
        result = subprocess.run(["curl", "-s", url], capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return {}  # return empty dict on failure

if __name__ == "__main__":
    # Load ratings file
    df = pd.read_csv("explicit_ratings_from_kafka.csv") # must contain user_id, movie_id

    # Fetch and expand user info (one row per rating row)
    user_dicts = df['user_id'].apply(get_user_info)

    user_info_df = pd.DataFrame(list(user_dicts))
    print(user_dicts)
    user_info_df["user_id"] = df["user_id"].values
    user_info_df.to_csv("users.csv", index=False)

    # Fetch and expand movie info (one row per rating row)
    movie_dicts = df['movie_id'].apply(get_movie_info)
    movie_info_df = pd.DataFrame(list(movie_dicts))
    movie_info_df["movie_id"] = df["movie_id"].values
    movie_info_df.to_csv("movies.csv", index=False)

    print("Created user.csv and movie.csv successfully!")