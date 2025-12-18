#!/bin/bash
# Adjust nginx weights to shift traffic between blue and green.

set -euo pipefail

PRIMARY=${1:-blue}
SECONDARY=green

if [[ "$PRIMARY" == "green" ]]; then
    SECONDARY=blue
elif [[ "$PRIMARY" != "blue" ]]; then
    echo "Error: color must be 'blue' or 'green'"
    exit 1
fi

echo "Switching traffic: primary=$PRIMARY (weight 3), secondary=$SECONDARY (weight 1)"

if [[ "$PRIMARY" == "blue" ]]; then
    HOST_CONF="nginx-blue-primary.conf"
else
    HOST_CONF="nginx-green-primary.conf"
fi

echo "Reloading recommender-loadbalancer..."
cp "$HOST_CONF" nginx-blue-primary.conf
docker exec recommender-loadbalancer nginx -s reload >/dev/null 2>&1 || docker restart recommender-loadbalancer

sleep 2
if curl -fs http://localhost:8080/nginx-health >/dev/null 2>&1; then
    echo "Traffic switch complete."
else
    echo "Warning: load balancer health endpoint not responding yet."
fi