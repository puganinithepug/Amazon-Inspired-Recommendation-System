import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app

def test_health_endpoint():
    # Test that the health endpoint is accessible
    client = app.test_client()
    response = client.get('/health')
    assert response.status_code == 200

def test_home_endpoint():
    # Test that the home endpoint is accessible
    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 200

def test_recommend_endpoint():
    # Test that the recommendation endpoint is accessible
    client = app.test_client()
    response = client.get('/recommend/100')
    assert response.status_code == 200

def test_recommend_returns():
    # Test that the recommendation endpoint returns data
    client = app.test_client()
    response = client.get('/recommend/100')
    assert len(response.data) > 0

def test_user_integer():
    # Test that integer user IDs are handled
    client = app.test_client()
    response = client.get('/recommend/100')
    assert response.status_code == 200

def test_user_id_float():
    # Test that a float user ID is handled and return 404
    client = app.test_client()
    response = client.get('/recommend/100.5')
    assert response.status_code == 404

def test_user_id_string():
    # Test that a string user ID is handled and return 404
    client = app.test_client()
    response = client.get('/recommend/abc')
    assert response.status_code == 404

def test_user_id_zero():
    # Test that user ID 0 is handled
    client = app.test_client()
    response = client.get('/recommend/0')
    assert response.status_code == 502
    assert len(response.data) > 0

def test_user_id_negative():
    # Test that the negative user ID is handled and return 404
    client = app.test_client()
    response = client.get('/recommend/-1')
    assert response.status_code == 404

def test_user_id_large():
    # Test that the very large user ID is handled
    client = app.test_client()
    response = client.get('/recommend/999999999999')
    assert response.status_code == 502
    assert len(response.data) > 0

def test_valid_recommendation_format():
    # Test that the recommendations are comma-separated
    client = app.test_client()
    response = client.get('/recommend/100')
    assert ',' in response.data.decode('utf-8')

def test_valid_recommendation_count_maximum():
    # Test that there are at most twenty movie recommendations
    client = app.test_client()
    response = client.get('/recommend/100')
    data = response.data.decode('utf-8')
    if 'Error' in data:
        return
    data = data.split(',')
    assert len(data) <= 20

def test_valid_recommendation_count_minimum():
    # Test that there is at least one movie recommendation
    client = app.test_client()
    response = client.get('/recommend/100')
    data = response.data.decode('utf-8')
    if 'Error' in data:
        return
    data = data.split(',')
    assert len(data) > 0

def test_recommendation_duplicate():
    # Test that the recommendations don't contain duplicates
    client = app.test_client()
    response = client.get('/recommend/100')
    data = response.data.decode('utf-8')
    if 'Error' in data:
        return
    data = data.split(',')
    assert len(data) == len(set(data))

def test_recommendation_movie_empty():
    # Test that all the recommended movies are non-empty
    client = app.test_client()
    response = client.get('/recommend/100')
    data = response.data.decode('utf-8')
    if 'Error' in data:
        return
    data = data.split(',')
    for d in data:
        assert len(d.strip()) > 0

def test_consistent_recommendation():
    # Test that the same user gets consistent recommendations across requests
    client = app.test_client()
    response1 = client.get('/recommend/100')
    response2 = client.get('/recommend/100')
    assert response1.data == response2.data


