#!/bin/bash
# Build and deploy a new model version to the blue or green environment.

set -euo pipefail

COLOR=${1:-green}
MODEL_VERSION=${2:-v1}

if [[ "$COLOR" != "blue" && "$COLOR" != "green" ]]; then
    echo "Error: color must be 'blue' or 'green'"
    exit 1
fi

echo "Deploying $COLOR environment with model version $MODEL_VERSION"

echo "Step 1/5: Building Docker image for recommender-$COLOR..."
MODEL_VERSION=$MODEL_VERSION docker-compose build recommender-$COLOR

echo "Step 2/5: Stopping old recommender-$COLOR container (if any)..."
docker stop recommender-$COLOR 2>/dev/null || true
docker rm recommender-$COLOR 2>/dev/null || true

echo "Step 3/5: Starting recommender-$COLOR..."
MODEL_VERSION=$MODEL_VERSION docker-compose up -d --no-deps --no-recreate recommender-$COLOR

echo "Step 4/5: Waiting for /health..."
for attempt in {1..12}; do
    if docker exec recommender-$COLOR curl -fs http://localhost:8080/health >/dev/null 2>&1; then
        echo "Health check passed on attempt $attempt"
        break
    fi
    if [[ $attempt -eq 12 ]]; then
        echo "Health check failed after 60s, rolling back..."
        docker logs recommender-$COLOR --tail 50 || true
        docker stop recommender-$COLOR || true
        docker rm recommender-$COLOR || true
        exit 1
    fi
    echo "  Attempt $attempt/12 failed, retrying in 5s..."
    sleep 5
done

echo "Step 5/5: Reloading nginx load balancer..."
docker exec recommender-loadbalancer nginx -s reload >/dev/null 2>&1 || true

echo "Deployment of recommender-$COLOR complete."
