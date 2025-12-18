from flask import Flask, request, jsonify

#from pathlib import Path

import requests

import pickle

import pandas as pd

import logging

from logging.handlers import TimedRotatingFileHandler

import json

from datetime import datetime

import time

import os



# Prometheus 

from prometheus_client import (

    Counter,

    Histogram,

    Gauge,

    Info,

    generate_latest,

    CONTENT_TYPE_LATEST,

)



app = Flask(__name__)



# Configure logging with 7-day rotation

# Create logs directory if it doesn't exist

import os

os.makedirs('/app/logs', exist_ok=True)



handler = TimedRotatingFileHandler(

    '/app/logs/recommendations.log',

    when='midnight',       # Rotate at midnight

    interval=1,            # Every 1 day

    backupCount=7          # Keep 7 days of logs

)

handler.setFormatter(logging.Formatter('%(message)s'))



logging.basicConfig(

    level=logging.INFO,

    handlers=[

        handler,

        logging.StreamHandler()

    ]

)




# Load model & data (kept from our version)



with open("models/svd_model.pkl", "rb") as f:

    model = pickle.load(f)



ratings = pd.read_csv("data/explicit_ratings_from_kafka.csv")

movies = pd.read_csv("data/movies.csv")

movies = movies[movies["message"] != "movie not found"]



PROV_PATH = f"models/{os.getenv('MODEL_VERSION')}_provenance.json"
MODELS_DIR = "models/"
files = [
    fname for fname in os.listdir(MODELS_DIR)
    if fname.endswith("_provenance.json")
]
files.sort()
PROV_PATH = os.path.join(MODELS_DIR, files[-1]) if files else None
#dir_path = Path(MODLES_DIR)
#candidates = sorted(dir_path.glob("*_provenance.json"))
#PROV_PATH =  str(candidates[-1]) else None


try:

    with open(PROV_PATH, "r") as f:

        PROVENANCE = json.load(f)

except Exception:

    PROVENANCE = {}





# Prometheus metrics


REQUEST_COUNT = Counter(

    "http_requests_total",

    "Total HTTP requests by method, endpoint, and status",

    ["method", "endpoint", "http_status"],

)

REQUEST_LATENCY = Histogram(

    "http_request_latency_seconds",

    "Request latency (seconds) by endpoint",

    ["endpoint"],

    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5),

)

RECS_SERVED = Counter(

    "recommendations_served_total",

    "Number of recommendations served",

    ["model_version"],

)

RECS_SIZE = Histogram(

    "recommendation_list_size",

    "Length of the recommendation list",

    buckets=(1, 3, 5, 10, 20, 50),

)

POS_RATE = Gauge(

    "pos_rate",

    "Positive rating rate (fraction 0..1)"

)

MODEL_INFO = Info("model_build_info", "Model metadata")

MODEL_INFO.info({

    "model_name": os.getenv("MODEL_NAME", "svd"),

    "model_version": os.getenv("MODEL_VERSION", "v1"),

})



def _observe_latency(endpoint_name: str, start_time: float):

    elapsed = time.time() - start_time

    REQUEST_LATENCY.labels(endpoint=endpoint_name).observe(elapsed)



def _inc_request(method: str, endpoint: str, status_code: int):

    REQUEST_COUNT.labels(method=method, endpoint=endpoint, http_status=str(status_code)).inc()





# Endpoints

@app.route("/recommend/<int:user_id>", methods=["GET"])

def recommend(user_id: int):

    """

    Returns comma-separated top movie_ids for the user.

    Logs the request as before, and now also emits Prometheus metrics.

    """

    endpoint_name = "/recommend"

    start_time = time.time()



    try:

        user_response = requests.get(

            f"http://fall2025-comp585.cs.mcgill.ca:8080/user/{user_id}",

            timeout=5

        )



        if user_response.status_code == 200:

            user_data = user_response.json()



            if "user_id" in user_data:

                rated_strings = ratings[ratings["user_id"] == user_id]["movie_id"].tolist()

                # Sort movie_strings to ensure deterministic iteration order

                movie_strings = sorted(movies["movie_id"].dropna().unique())



                predictions = []

                for movie_string in movie_strings:

                    if movie_string not in rated_strings:

                        pred = model.predict(user_id, movie_string)

                        predictions.append((movie_string, pred.est))



                # Sort by prediction score (descending), then by movie_id (ascending) for deterministic tie-breaking

                predictions.sort(key=lambda x: (-x[1], x[0]))



                top_movie_strings = [m for m, _ in predictions[:20]]

                tmdb_ids = movies[movies["movie_id"].isin(top_movie_strings)]["tmdb_id"].drop_duplicates().tolist()

                tmdb_ids = [int(x) for x in tmdb_ids]



                # business logging (unchanged)

                latency_ms = (time.time() - start_time) * 1000

                logging.info(json.dumps({

                    "timestamp": datetime.utcnow().isoformat(),

                    "user_id": user_id,

                    "movie_ids": top_movie_strings,

                    "tmdb_ids": tmdb_ids,

                    "latency_ms": latency_ms,

                    "status": "success",

                    #"model_version": os.getenv("MODEL_VERSION"),

                    "model_version":PROVENANCE.get("model_version"),
                    "deployment_color": os.getenv("DEPLOYMENT_COLOR"),

                    "pipeline_git_sha": PROVENANCE.get("pipeline_git_sha"),

                    "training_data": PROVENANCE.get("training_data"),

                    "framework_versions": PROVENANCE.get("framework_versions")

                }))



                # prometheus metrics

                RECS_SERVED.labels(model_version=os.getenv("MODEL_VERSION", "v1")).inc()

                RECS_SIZE.observe(len(top_movie_strings))

                _observe_latency(endpoint_name, start_time)

                _inc_request("GET", endpoint_name, 200)



                # Add header to show which backend handled the request

                response = ",".join(top_movie_strings)

                headers = {"X-Backend-Color": os.getenv("DEPLOYMENT_COLOR", "unknown")}

                return response, 200, headers



            elif "message" in user_data:

                latency_ms = (time.time() - start_time) * 1000

                logging.warning(json.dumps({

                    "timestamp": datetime.utcnow().isoformat(),

                    "user_id": user_id,

                    "status": "user_not_found",

                    "latency_ms": latency_ms,

                    "message": user_data["message"],

                }))

                _observe_latency(endpoint_name, start_time)

                _inc_request("GET", endpoint_name, 404)

                return user_data["message"], 404



        # API call failed

        latency_ms = (time.time() - start_time) * 1000

        logging.error(json.dumps({

            "timestamp": datetime.utcnow().isoformat(),

            "user_id": user_id,

            "status": "error",

            "latency_ms": latency_ms,

            "error": "API call failed"

        }))

        _observe_latency(endpoint_name, start_time)

        _inc_request("GET", endpoint_name, 502)

        return "Error!", 502



    except Exception as e:

        latency_ms = (time.time() - start_time) * 1000

        logging.error(json.dumps({

            "timestamp": datetime.utcnow().isoformat(),

            "user_id": user_id,

            "status": "exception",

            "latency_ms": latency_ms,

            "error": str(e)

        }))

        _observe_latency(endpoint_name, start_time)

        _inc_request("GET", endpoint_name, 500)

        return "Error!", 500



@app.route("/telemetry/online_quality", methods=["POST"])

def update_online_quality():

    """

    Optional: your pipeline (or a cron job) can POST {"value": <float>}

    to push an online quality metric (e.g., positive_rating_rate).

    """

    endpoint_name = "/telemetry/online_quality"

    start_time = time.time()

    try:

        data = request.get_json(force=True) or {}

        value = float(data.get("value", 0.0))

        ONLINE_QUALITY.set(value)

        _observe_latency(endpoint_name, start_time)

        _inc_request("POST", endpoint_name, 200)

        return jsonify({"ok": True}), 200

    except Exception as e:

        _observe_latency(endpoint_name, start_time)

        _inc_request("POST", endpoint_name, 400)

        return jsonify({"ok": False, "error": str(e)}), 400



@app.route("/metrics", methods=["GET"])

def metrics():

    """Prometheus scrape endpoint."""

    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}



@app.route("/health", methods=["GET"])

def health_check():

    """Healthcheck endpoint for load balancer and monitoring."""

    return jsonify({

        "status": "ok",

        "model_version": os.getenv("MODEL_VERSION", "v1"),

        "deployment_color": os.getenv("DEPLOYMENT_COLOR", "unknown")

    }), 200



@app.route("/version", methods=["GET"])

def version():

    """Version information endpoint."""

    return jsonify({

        "model_name": os.getenv("MODEL_NAME", "svd"),

        "model_version": os.getenv("MODEL_VERSION", "v1"),

        "deployment_color": os.getenv("DEPLOYMENT_COLOR", "unknown"),

        "python_version": "3.10",

        "service": "movie-recommender"

    }), 200



@app.route("/", methods=["GET"])

def home():

    """Serve the frontend HTML page."""

    html_content = """

<!DOCTYPE html>

<html lang="en">

<head>

    <meta charset="UTF-8">

    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Movie Recommendation Service</title>

    <style>

        * {

            margin: 0;

            padding: 0;

            box-sizing: border-box;

        }

        body {

            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;

            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);

            min-height: 100vh;

            display: flex;

            justify-content: center;

            align-items: center;

            padding: 20px;

        }

        .container {

            background: white;

            border-radius: 20px;

            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);

            padding: 40px;

            max-width: 800px;

            width: 100%;

        }

        h1 {

            color: #333;

            margin-bottom: 10px;

            font-size: 2.5em;

            text-align: center;

        }

        .subtitle {

            color: #666;

            text-align: center;

            margin-bottom: 30px;

            font-size: 0.9em;

        }

        .form-group {

            margin-bottom: 25px;

        }

        label {

            display: block;

            margin-bottom: 8px;

            color: #333;

            font-weight: 600;

            font-size: 1.1em;

        }

        input[type="number"] {

            width: 100%;

            padding: 15px;

            border: 2px solid #e0e0e0;

            border-radius: 10px;

            font-size: 1.1em;

            transition: border-color 0.3s;

        }

        input[type="number"]:focus {

            outline: none;

            border-color: #667eea;

        }

        button {

            width: 100%;

            padding: 15px;

            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);

            color: white;

            border: none;

            border-radius: 10px;

            font-size: 1.2em;

            font-weight: 600;

            cursor: pointer;

            transition: transform 0.2s, box-shadow 0.2s;

        }

        button:hover {

            transform: translateY(-2px);

            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);

        }

        button:active {

            transform: translateY(0);

        }

        button:disabled {

            background: #ccc;

            cursor: not-allowed;

            transform: none;

        }

        .results {

            margin-top: 30px;

            padding: 20px;

            background: #f8f9fa;

            border-radius: 10px;

            display: none;

        }

        .results.show {

            display: block;

        }

        .results h2 {

            color: #333;

            margin-bottom: 15px;

            font-size: 1.5em;

        }

        .movie-list {

            list-style: none;

        }

        .movie-item {

            padding: 12px;

            margin-bottom: 8px;

            background: white;

            border-radius: 8px;

            border-left: 4px solid #667eea;

            font-size: 1.1em;

            color: #333;

        }

        .error {

            color: #e74c3c;

            background: #ffeaea;

            padding: 15px;

            border-radius: 10px;

            margin-top: 20px;

            border-left: 4px solid #e74c3c;

            display: none;

        }

        .error.show {

            display: block;

        }

        .loading {

            text-align: center;

            color: #667eea;

            margin-top: 20px;

            display: none;

        }

        .loading.show {

            display: block;

        }

        .spinner {

            border: 3px solid #f3f3f3;

            border-top: 3px solid #667eea;

            border-radius: 50%;

            width: 30px;

            height: 30px;

            animation: spin 1s linear infinite;

            margin: 0 auto 10px;

        }

        @keyframes spin {

            0% { transform: rotate(0deg); }

            100% { transform: rotate(360deg); }

        }

    </style>

</head>

<body>

    <div class="container">

        <h1>🎬 Movie Recommendations</h1>

        <p class="subtitle">Get personalized movie recommendations for any user</p>



        <form id="recommendForm">

            <div class="form-group">

                <label for="userId">User ID:</label>

                <input type="number" id="userId" name="userId" min="1" placeholder="Enter user ID (e.g., 4)" required>

            </div>

            <button type="submit" id="submitBtn">Get Recommendations</button>

        </form>



        <div class="loading" id="loading">

            <div class="spinner"></div>

            <p>Loading recommendations...</p>

        </div>



        <div class="error" id="error"></div>



        <div class="results" id="results">

            <h2>Recommended Movies:</h2>

            <ul class="movie-list" id="movieList"></ul>

        </div>

    </div>



    <script>

        const form = document.getElementById('recommendForm');

        const userIdInput = document.getElementById('userId');

        const submitBtn = document.getElementById('submitBtn');

        const resultsDiv = document.getElementById('results');

        const movieList = document.getElementById('movieList');

        const errorDiv = document.getElementById('error');

        const loadingDiv = document.getElementById('loading');



        form.addEventListener('submit', async (e) => {

            e.preventDefault();



            const userId = userIdInput.value;

            if (!userId) return;



            // Reset UI

            resultsDiv.classList.remove('show');

            errorDiv.classList.remove('show');

            loadingDiv.classList.add('show');

            submitBtn.disabled = true;

            movieList.innerHTML = '';



            try {

                const response = await fetch(`/recommend/${userId}`);



                if (!response.ok) {

                    const errorText = await response.text();

                    throw new Error(errorText || `HTTP ${response.status}`);

                }



                const data = await response.text();

                const movieIds = data.split(',').filter(id => id.trim());



                // Get backend info from response header

                const backendColor = response.headers.get('X-Backend-Color') || 'unknown';



                if (movieIds.length === 0) {

                    throw new Error('No recommendations found');

                }



                // Display backend info

                const backendInfo = document.createElement('p');

                backendInfo.style.cssText = 'color: #666; font-size: 0.9em; margin-bottom: 10px; font-style: italic;';

                backendInfo.textContent = `Handled by: ${backendColor} backend`;



                // Display results

                movieIds.forEach((movieId, index) => {

                    const li = document.createElement('li');

                    li.className = 'movie-item';

                    li.textContent = `${index + 1}. Movie ID: ${movieId.trim()}`;

                    movieList.appendChild(li);

                });



                // Clear previous backend info and add new one

                const existingBackendInfo = resultsDiv.querySelector('p');

                if (existingBackendInfo) {

                    existingBackendInfo.remove();

                }

                resultsDiv.insertBefore(backendInfo, movieList);



                resultsDiv.classList.add('show');

                loadingDiv.classList.remove('show');



            } catch (error) {

                errorDiv.textContent = `Error: ${error.message}`;

                errorDiv.classList.add('show');

                loadingDiv.classList.remove('show');

            } finally {

                submitBtn.disabled = false;

            }

        });

    </script>

</body>

</html>

    """

    return html_content, 200



if __name__ == "__main__":

    app.run(host="0.0.0.0", port=8080, debug=False)
