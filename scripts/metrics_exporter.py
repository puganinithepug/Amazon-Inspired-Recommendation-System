#!/usr/bin/env python3
"""
Expose online_evaluation.py results in online_metrics.json as Prometheus metrics.

Usage:
  python scripts/metrics_exporter.py --file /metrics-data/online_metrics.json --port 9108
"""
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST

def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None

class MetricsServer(BaseHTTPRequestHandler):
    json_path = None

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404); self.end_headers(); return

        data = load_json(self.json_path)
        reg = CollectorRegistry()

        g_timestamp = Gauge("online_metrics_timestamp_seconds", "UTC timestamp from online_metrics.json", registry=reg)
        g_cov_users_reco = Gauge("online_total_users_with_recommendations", "Users who received at least one recommendation", registry=reg)
        g_cov_users_ratings = Gauge("online_total_users_with_ratings", "Users who produced at least one rating", registry=reg)
        g_users_who_rated_reco = Gauge("online_users_who_rated_recommendations", "Users who rated recommended items", registry=reg)
        g_movies_recommended = Gauge("online_movies_recommended", "Count of recs shown", registry=reg)
        g_movies_rated = Gauge("online_movies_rated", "Count of recs that were later rated", registry=reg)
        g_positive_ratings = Gauge("online_positive_ratings", "Ratings >= 4.0 on recs", registry=reg)
        g_total_rating_value = Gauge("online_total_rating_value", "Sum of rating values on recs", registry=reg)
        g_rating_rate = Gauge("online_rating_rate", "movies_rated / movies_recommended", registry=reg)
        g_positive_rating_rate = Gauge("online_positive_rating_rate", "positive_ratings / movies_rated", registry=reg)
        g_average_rating = Gauge("online_average_rating", "Avg rating on recommended movies", registry=reg)
        g_user_engagement_rate = Gauge("online_user_engagement_rate",
                                       "users_who_rated_recommendations / total_users_with_recommendations", registry=reg)
        # Alias for dashboards/alerts
        g_quality = Gauge("online_quality_score", "Alias of primary quality metric (uses positive_rating_rate)", registry=reg)

        if data and "metrics" in data:
            m = data["metrics"]
            try:
                import datetime
                ts = data.get("timestamp")
                if ts:
                    dt = datetime.datetime.fromisoformat(ts.replace("Z",""))
                    g_timestamp.set(dt.timestamp())
            except Exception:
                pass

            g_cov_users_reco.set(m.get("total_users_with_recommendations", 0))
            g_cov_users_ratings.set(m.get("total_users_with_ratings", 0))
            g_users_who_rated_reco.set(m.get("users_who_rated_recommendations", 0))
            g_movies_recommended.set(m.get("movies_recommended", 0))
            g_movies_rated.set(m.get("movies_rated", 0))
            g_positive_ratings.set(m.get("positive_ratings", 0))
            g_total_rating_value.set(m.get("total_rating_value", 0.0))
            g_rating_rate.set(m.get("rating_rate", 0.0))
            g_positive_rating_rate.set(m.get("positive_rating_rate", 0.0))
            g_average_rating.set(m.get("average_rating", 0.0))
            g_user_engagement_rate.set(m.get("user_engagement_rate", 0.0))
            g_quality.set(m.get("positive_rating_rate", 0.0))

        out = generate_latest(reg)
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=os.environ.get("ONLINE_METRICS_FILE", "/metrics-data/online_metrics.json"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("EXPORTER_PORT", "9108")))
    args = ap.parse_args()

    MetricsServer.json_path = args.file
    server = HTTPServer(("0.0.0.0", args.port), MetricsServer)
    print(f"[exporter] Serving metrics from {args.file} on :{args.port}/metrics")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
