import pytest
import pandas as pd
import pickle
import os
from pathlib import Path

def test_kafka_file_exists():
    # Test that kafka file exists in the data folder
    assert os.path.exists('data/explicit_ratings_from_kafka.csv')

def test_movie_file_exists():
    # Test that movie file exists in the data folder
    assert os.path.exists('data/movies.csv')

def test_movie_file_empty():
    # Test that movie file is not empty
    df = pd.read_csv('data/movies.csv', nrows=2)
    assert len(df) > 0

def test_movie_file_movie_id():
    # Test that movie file has movie id column
    df = pd.read_csv('data/movies.csv', nrows=2)
    assert 'movie_id' in df.columns

def test_movie_file_title():
    # Test that movie file has title column
    df = pd.read_csv('data/movies.csv', nrows=2)
    assert 'title' in df.columns

def test_kafka_file_empty():
    # Test that kafka file is not empty
    df = pd.read_csv('data/explicit_ratings_from_kafka.csv', nrows = 2)
    assert len(df) > 0

def test_kafka_file_user_id():
    # Test that kafka file has user id column
    df = pd.read_csv('data/explicit_ratings_from_kafka.csv', nrows = 2)
    assert 'user_id' in df.columns

def test_kafka_file_movie_id():
    # Test that kafka file has movie id column
    df = pd.read_csv('data/explicit_ratings_from_kafka.csv', nrows = 2)
    assert 'movie_id' in df.columns

def test_kafka_file_rating():
    # Test that kafka file has rating column
    df = pd.read_csv('data/explicit_ratings_from_kafka.csv', nrows = 2)
    assert 'rating' in df.columns

def test_kafka_file_timestamp():
    # Test that kafka file has timestamp column
    df = pd.read_csv('data/explicit_ratings_from_kafka.csv', nrows = 2)
    assert 'timestamp' in df.columns
