#!/bin/bash
# Orchestrate a blue/green deployment with gradual traffic shifting.

set -euo pipefail

NEW_VERSION=${1:-v1}

echo "Preparing blue/green deployment for model version $NEW_VERSION"

get_version() {
    local color=$1
    docker exec recommender-$color curl -fs http://localhost:8080/health 2>/dev/null | \
        grep -o '"model_version":"[^"]*"' | cut -d'"' -f4 || echo "v0"
}

BLUE_VERSION=$(get_version blue)
GREEN_VERSION=$(get_version green)

if [[ "$GREEN_VERSION" > "$BLUE_VERSION" ]]; then
    TARGET=blue
    CURRENT=green
else
    TARGET=green
    CURRENT=blue
fi

echo "Current primary: $CURRENT (version $([[ $CURRENT == blue ]] && echo $BLUE_VERSION || echo $GREEN_VERSION))"
echo "Deploying new version to $TARGET"

read -p "Continue? (y/N) " -n 1 -r
echo
[[ $REPLY =~ ^[Yy]$ ]] || exit 1

./scripts/deploy-blue-green.sh $TARGET $NEW_VERSION

echo "Running smoke test via load balancer..."
sleep 5
if ! curl -fs http://localhost:8080/health >/dev/null 2>&1; then
    echo "Smoke test failed. Rolling back."
    docker stop recommender-$TARGET || true
    exit 1
fi

echo "Phase 1: 75% $CURRENT / 25% $TARGET"
./scripts/switch-traffic.sh $CURRENT
sleep 30

echo "Phase 2: 50% / 50%"

cp nginx-equal.conf nginx-blue-primary.conf
docker exec recommender-loadbalancer nginx -s reload || docker restart recommender-loadbalancer
sleep 30

echo "Phase 3: 25% $CURRENT / 75% $TARGET"
./scripts/switch-traffic.sh $TARGET
sleep 30

read -p "Switch 100% traffic to $TARGET? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ./scripts/switch-traffic.sh $TARGET
    docker stop recommender-$CURRENT || true
    echo "$CURRENT stopped. Deployment complete."
else
    echo "Leaving traffic split at 75/25. You can rerun switch-traffic.sh when ready."
fi