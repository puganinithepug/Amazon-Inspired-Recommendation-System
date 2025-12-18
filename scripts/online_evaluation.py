#!/usr/bin/env python3
"""
Online Evaluation Script for Movie Recommendation System
"""

import subprocess
import json
import re
import time
import os
from datetime import datetime, timedelta
from collections import defaultdict
from kafka import KafkaConsumer


def load_recommendations_from_file(log_file_path='/recommendation-logs/recommendations.log'):
    """Load recommendation logs from shared volume."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Loading recommendations from {log_file_path}...")
    recommendations = {}

    try:
        if not os.path.exists(log_file_path):
            print(f" Warning: Log file not found at {log_file_path}")
            return {}

        with open(log_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith('{'):
                    continue
                try:
                    log = json.loads(line)
                    if log.get('status') == 'success':
                        user_id = log['user_id']
                        timestamp = datetime.fromisoformat(log['timestamp'])
                        movie_ids = log['movie_ids']
                        if user_id not in recommendations or timestamp > recommendations[user_id]['timestamp']:
                            recommendations[user_id] = {
                                'timestamp': timestamp,
                                'movie_ids': set(movie_ids),
                                'tmdb_ids': log.get('tmdb_ids', [])
                            }
                except (json.JSONDecodeError, KeyError) as e:
                    continue

        print(f" → Loaded {len(recommendations)} user recommendations")
        return recommendations
    except Exception as e:
        print(f"Error reading log file: {e}")
        return {}


def collect_ratings_from_kafka(max_duration_seconds=300, kafka_server='fall2025-comp585.cs.mcgill.ca:9092'):
    """Collect all rating events from Kafka stream."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Collecting all ratings from Kafka...")
    user_ratings = defaultdict(list)
    try:
        consumer = KafkaConsumer(
            'movielog2',
            bootstrap_servers=[kafka_server],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='online_evaluator_team2',
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        count = 0
        start_time = time.time()
        last_message_time = time.time()
        idle_threshold = 5

        while time.time() - start_time < max_duration_seconds:
            message_batch = consumer.poll(timeout_ms=1000)
            if message_batch:
                last_message_time = time.time()
                for _, messages in message_batch.items():
                    for message in messages:
                        if not message.value:
                            continue
                        rating_match = re.match(
                            r'^([^,]+),(\d+),GET /rate/([^=]+)=([0-9.]+)',
                            message.value
                        )
                        if rating_match:
                            timestamp_str, user_id_str, movie_id, rating_str = rating_match.groups()
                            try:
                                user_id = int(user_id_str)
                                rating = float(rating_str)
                                timestamp = datetime.fromisoformat(timestamp_str.replace('T', ' ').split('.')[0])
                                user_ratings[user_id].append((movie_id, rating, timestamp))
                                count += 1
                                if count % 1000 == 0:
                                    print(f" → Processed {count} ratings...")
                            except ValueError:
                                continue
            else:
                if time.time() - last_message_time > idle_threshold:
                    print(f" → No new messages for {idle_threshold}s, finished reading stream")
                    break

        consumer.close()
        print(f" → Collected {count} ratings from {len(user_ratings)} users")
        return dict(user_ratings)
    except Exception as e:
        print(f" Error collecting from Kafka: {e}")
        return {}


def compute_metrics(recommendations, ratings, time_window_hours=168):
    """Compute online evaluation metrics."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Computing metrics...")
    metrics = {
        'total_users_with_recommendations': len(recommendations),
        'total_users_with_ratings': len(ratings),
        'movies_recommended': 0,
        'movies_rated': 0,
        'positive_ratings': 0,
        'total_rating_value': 0.0,
        'users_who_rated_recommendations': 0
    }

    time_window = timedelta(hours=time_window_hours)

    # Debug: Find users with both recommendations and ratings
    users_with_both = set(recommendations.keys()) & set(ratings.keys())
    print(f" → Users with both recommendations AND ratings: {len(users_with_both)}")

    # Debug: Sample a user to see what's happening
    debug_count = 0
    timestamp_mismatches = 0
    movie_id_mismatches = 0
    successful_matches = 0

    for user_id, rec_data in recommendations.items():
        rec_timestamp = rec_data['timestamp']
        rec_movies = rec_data['movie_ids']
        metrics['movies_recommended'] += len(rec_movies)

        if user_id not in ratings:
            continue

        # Debug first few users with both
        if debug_count < 3:
            print(f"\n   DEBUG User {user_id}:")
            print(f"     Rec timestamp: {rec_timestamp}")
            print(f"     Rec movies (first 3): {list(rec_movies)[:3]}")
            user_rating_data = ratings[user_id][:3]
            print(f"     User ratings (first 3): {[(m, t) for m, r, t in user_rating_data]}")
            debug_count += 1

        rated_recommendations = []
        for movie_id, rating, rating_timestamp in ratings[user_id]:
            # Check each condition separately for debugging
            time_check = rating_timestamp > rec_timestamp
            window_check = rating_timestamp - rec_timestamp <= time_window if time_check else False
            movie_check = movie_id in rec_movies

            if not time_check:
                timestamp_mismatches += 1
            elif not window_check:
                pass  # Outside window
            elif not movie_check:
                movie_id_mismatches += 1
            else:
                # All checks passed!
                successful_matches += 1
                rated_recommendations.append((movie_id, rating))
                metrics['movies_rated'] += 1
                metrics['total_rating_value'] += rating
                if rating >= 4.0:
                    metrics['positive_ratings'] += 1

        if rated_recommendations:
            metrics['users_who_rated_recommendations'] += 1

    print(f"\n → DEBUG Summary:")
    print(f"   - Timestamp mismatches (rating before rec): {timestamp_mismatches}")
    print(f"   - Movie ID mismatches (rated movie not in recs): {movie_id_mismatches}")
    print(f"   - Successful matches: {successful_matches}\n")

    metrics['rating_rate'] = (
        metrics['movies_rated'] / metrics['movies_recommended']
        if metrics['movies_recommended'] > 0 else 0.0
    )
    metrics['positive_rating_rate'] = (
        metrics['positive_ratings'] / metrics['movies_rated']
        if metrics['movies_rated'] > 0 else 0.0
    )
    metrics['average_rating'] = (
        metrics['total_rating_value'] / metrics['movies_rated']
        if metrics['movies_rated'] > 0 else 0.0
    )
    metrics['user_engagement_rate'] = (
        metrics['users_who_rated_recommendations'] /
        metrics['total_users_with_recommendations']
        if metrics['total_users_with_recommendations'] > 0 else 0.0
    )

    return metrics


def save_metrics(metrics, output_file='online_metrics.json'):
    metrics_output = {'timestamp': datetime.utcnow().isoformat(), 'metrics': metrics}
    with open(output_file, 'w') as f:
        json.dump(metrics_output, f, indent=2)
    print(f" → Metrics saved to {output_file}")


def append_metrics_history(metrics, history_file='online_metrics_history.jsonl'):
    metrics_entry = {'timestamp': datetime.utcnow().isoformat(), 'metrics': metrics}
    with open(history_file, 'a') as f:
        f.write(json.dumps(metrics_entry) + '\n')


def print_metrics(metrics):
    """Pretty print metrics."""

    print("\n" + "="*30)
    print("ONLINE EVALUATION METRICS")
    print("="*60)

    print(f"\n Coverage:")
    print(f"  # of unique users who received recommendation: {metrics['total_users_with_recommendations']}")
    print(f"  # of unique users whos rated movies: {metrics['total_users_with_ratings']}")
    print(f"  Users who rated our recommendations: {metrics['users_who_rated_recommendations']}")

    print(f"\n Engagement (how much users interated with our recommendation):")
    print(f"  Movies recommended: {metrics['movies_recommended']}")
    print(f"  Movies rated: {metrics['movies_rated']}")
    print(f"  Rating rate: {metrics['rating_rate']:.2%}")
    print(f"  Percentage of users who engaged with our recommendation: {metrics['user_engagement_rate']:.2%}")

    print(f"\n Quality:")
    print(f"  Positive ratings (>=4.0): {metrics['positive_ratings']}")
    print(f"  Positive rating rate: {metrics['positive_rating_rate']:.2%}")
    print(f"  Average rating: {metrics['average_rating']:.2f}")
    print("=" * 60 + "\n")


def main():
    print("=" * 60)
    print("Movie Recommendation System - Online Evaluation")
    print("=" * 60)
    print(f"Started at {datetime.now():%Y-%m-%d %H:%M:%S}\n")

    log_file = os.environ.get('RECOMMENDATION_LOG_FILE', '/recommendation-logs/recommendations.log')
    kafka_server = os.environ.get('KAFKA_BROKER', 'fall2025-comp585.cs.mcgill.ca:9092')

    print(f"Using log file: {log_file}")
    print(f"Using Kafka server: {kafka_server}\n")

    recommendations = load_recommendations_from_file(log_file_path=log_file)
    if not recommendations:
        print(" No recommendations found in container logs.")
        return

    ratings = collect_ratings_from_kafka(kafka_server=kafka_server)
    if not ratings:
        print(" No ratings found in Kafka stream.")
        return

    metrics = compute_metrics(recommendations, ratings)
    print_metrics(metrics)
    save_metrics(metrics)
    append_metrics_history(metrics)
    print(" Evaluation complete\n")


if __name__ == "__main__":
    main()