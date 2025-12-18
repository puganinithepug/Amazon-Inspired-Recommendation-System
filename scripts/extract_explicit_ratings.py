import argparse
import csv
import os
import sys
import re
import time
from kafka import KafkaConsumer

from datetime import datetime
import pandas as pd

def edgecase_timestamp(ts: str) -> str:

    if not isinstance(ts, str):
        return ts
    s = ts.strip()

    # Match date (YYYY-MM-DD), separator (T or space), hour (1-2 digits), minute (2 digits)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{1,2}):(\d{2})$", s)
    if m:
        date_part, hour, minute = m.groups()
        return f"{date_part}T{int(hour):02d}:{minute}:00"

    # If it already contains seconds (e.g. ...:MM:SS) or doesn't match the short pattern,
    # return the trimmed original.
    return s


def _open_writer(path: str, append: bool):
    mode = "a" if append and os.path.exists(path) else "w"
    outfile = open(path, mode, newline='', encoding='utf-8')
    writer = csv.writer(outfile)
    if mode == "w" or (mode == "a" and os.path.getsize(path) == 0):
        writer.writerow(['user_id', 'movie_id', 'rating', 'timestamp'])
        print("CSV header written: user_id, movie_id, rating, timestamp")
    return outfile, writer


def extract_explicit_ratings_from_stream(kafka_server, output_file, max_rows=100000, team_number=2,
                                         append=False, max_duration=60):
    full_topic = f"movielog{team_number}"
    # set to 0
    ratings_extracted=0
    start_time = time.time()
    try:
        consumer = KafkaConsumer(
            full_topic,
            bootstrap_servers=[kafka_server],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='explicit_rating_extractor',
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=5000  # exit if idle for 5s
        )

        outfile, writer = _open_writer(output_file, append)
        with outfile:

            for message in consumer:
                if message.value is None:
                    continue

                line = message.value.strip()
                rating_match = re.match(r'^([^,]+),(\d+),GET /rate/([^=]+)=([0-9.]+)', line)

                if rating_match:
                    raw_ts = edgecase_timestamp(rating_match.group(1))
                    user_id = rating_match.group(2)
                    movie_id = rating_match.group(3)
                    rating = rating_match.group(4)

                    timestamp = raw_ts if raw_ts else None
                    if timestamp is None:
                        print(f"Skipped invalid/empty timestamp: {raw_ts!r}")
                        continue
                    else:
                        if ratings_extracted < 5:
                            print(f"Timestamp (edge-case fixed if needed): {raw_ts}")

                    try:
                        float(rating)
                        writer.writerow([user_id, movie_id, rating, timestamp])
                        ratings_extracted += 1
                        if ratings_extracted % 100 == 0:
                            print(f"Extracted {ratings_extracted} ratings...")
                        elif ratings_extracted <= 10:
                            print(f"Rating #{ratings_extracted}: User {user_id} rated '{movie_id}' = {rating}")

                        if max_rows and ratings_extracted >= max_rows:
                            break
                    except ValueError:
                        continue

                elapsed = time.time() - start_time
                if elapsed >= max_duration:
                    print(f"Reached max_duration={max_duration}s, stopping stream ingestion.")
                    break

    except KeyboardInterrupt:
        print(f"Extraction interrupted")

    except Exception as e:
        return False

    finally:
        try:
            consumer.close()
        except:
            pass

    return True

def extract_from_beginning(kafka_server, topic, output_file, max_rows=100000, team_number=2,
                           append=False):
    full_topic = f"movielog{team_number}"
    ratings_extracted = 0

    try:
        # Create Kafka consumer starting from beginning
        consumer = KafkaConsumer(
            full_topic,
            bootstrap_servers=[kafka_server],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='explicit_rating_extractor_historical',
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=10000
        )

        print("✓ Connected to Kafka stream successfully!")

        outfile, writer = _open_writer(output_file, append)
        with outfile:
            for message in consumer:
                if message.value is None:
                    continue

                line = message.value.strip()
                rating_match = re.match(r'^([^,]+),(\d+),GET /rate/([^=]+)=([0-9.]+)', line)

                if rating_match:
                    #timestamp = rating_match.group(1)
                    #raw_ts = rating_match.group(1)
                    raw_ts = edgecase_timestamp(rating_match.group(1))

                    user_id = rating_match.group(2)
                    movie_id = rating_match.group(3)
                    rating = rating_match.group(4)

                    # Use the (fixed) raw timestamp directly — skip if empty/falsy
                    timestamp = raw_ts if raw_ts else None
                    if timestamp is None:
                        print(f"Skipped invalid/empty timestamp: {raw_ts!r}")
                        continue
                    else:
                        if ratings_extracted < 5:  # show first few only
                            print(f"Timestamp (edge-case fixed if needed): {raw_ts}")

                    try:
                        float(rating)
                        writer.writerow([user_id, movie_id, rating, timestamp])
                        ratings_extracted += 1
                        if ratings_extracted % 1000 == 0:
                            print(f"Extracted {ratings_extracted} ratings...")
                        elif ratings_extracted <= 10:
                            print(f"Rating #{ratings_extracted}: User {user_id} rated '{movie_id}' = {rating}")

                        if max_rows and ratings_extracted >= max_rows:
                            break

                    except ValueError:
                        continue

    except Exception as e:
        return False

    finally:
        try:
            consumer.close()
        except:
            pass
    return True

def build_parser():
    parser = argparse.ArgumentParser(
        description="Extract explicit user ratings from the COMP585 Kafka topics."
    )
    parser.add_argument(
        "--mode",
        choices=["stream", "historical"],
        help="Mode to read from the Kafka topic. Defaults to interactive prompt.",
    )
    parser.add_argument(
        "--kafka-server",
        default="fall2025-comp585.cs.mcgill.ca:9092",
        help="Kafka bootstrap server address.",
    )
    parser.add_argument(
        "--team-number",
        type=int,
        default=2,
        help="Team number for the movielog topic (movielog<team-number>).",
    )
    parser.add_argument(
        "--output-file",
        default="data/explicit_ratings_from_kafka.csv",
        help="Destination CSV file.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100000,
        help="Maximum number of rows to export (0 = unlimited).",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the output file instead of overwriting it.",
    )
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.mode:
        print("=" * 80)
        print("Kafka Data Stream")
        print("=" * 80)
        print("\nSelect extraction mode:")
        print("1. Real-time stream (latest messages)")
        print("2. Historical data (from beginning of topic)")
        choice = input("Enter choice (1 or 2): ").strip()
        if choice == "1":
            args.mode = "stream"
        elif choice == "2":
            args.mode = "historical"
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)

    if args.mode == "stream":
        print("\n🔄 Starting real-time extraction...")
        success = extract_explicit_ratings_from_stream(
            args.kafka_server,
            args.output_file,
            args.max_rows,
            args.team_number,
            append=args.append,
        )
    else:
        print("\n📚 Starting historical extraction...")
        success = extract_from_beginning(
            args.kafka_server,
            None,
            args.output_file,
            args.max_rows,
            args.team_number,
            append=args.append,
        )

    if success:
        print("\n✅ Extraction completed successfully!")
        print(f"Check the file: {args.output_file}")
    else:
        print("\n❌ Extraction failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
