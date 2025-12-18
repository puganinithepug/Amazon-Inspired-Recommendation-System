#!/usr/bin/env python3
"""
Automates the data ingestion → training → evaluation → canary deployment workflow.

Run with `python scripts/automate_release.py --execute --metrics-out` to perform the full release,
or without --execute to preview the steps (dry-run).
or without --metrics-out to ignore offline metrics log.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import hashlib
import surprise

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON_BIN = os.environ.get("PYTHON_BIN", sys.executable)


def run_cmd(cmd: List[str], dry_run: bool, cwd: Optional[Path] = None, check: bool = True):
    display = " ".join(cmd)
    print(f"\n$ {display}")
    if dry_run:
        print("  (dry-run) command skipped")
        return subprocess.CompletedProcess(cmd, 0)
    return subprocess.run(cmd, check=check, cwd=str(cwd) if cwd else None)


def append_release_log(log_path: Path, entry: dict):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def wait_for_health(url: str, attempts: int, interval: float, dry_run: bool) -> bool:
    if dry_run:
        print(f"(dry-run) Skipping health checks for {url}")
        return True
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                print(f"Health check #{attempt} OK ({url})")
                return True
            print(f"Health check #{attempt} failed: {resp.status_code}")
        except requests.RequestException as exc:
            print(f"Health check #{attempt} error: {exc}")
        time.sleep(interval)
    return False


def stop_container(name: str, dry_run: bool):
    if not name:
        return
    run_cmd(["docker", "rm", "-f", name], dry_run=dry_run, check=False)


def check_online_metric(url: str, metric_name: str, minimum: float) -> bool:
    if not url or minimum is None:
        return True
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if line.startswith(metric_name):
                parts = line.split()
                if len(parts) >= 2:
                    value = float(parts[1])
                    print(f"Online metric {metric_name}={value:.4f} (threshold {minimum})")
                    return value >= minimum
        print(f"Metric {metric_name} not found in {url}")
        return False
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Failed to fetch online metrics: {exc}")
        return False


def apply_equal_weights():
    tmp_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    tmp_file.write(
        "events { worker_connections 1024; }\n"
        "http {\n"
        "  upstream recommender_backend {\n"
        "    least_conn;\n"
        "    server recommender-blue:8080 weight=1 max_fails=3 fail_timeout=30s;\n"
        "    server recommender-green:8080 weight=1 max_fails=3 fail_timeout=30s;\n"
        "  }\n"
        "  server {\n"
        "    listen 80;\n"
        "    location /nginx-health { return 200 \"healthy\\n\"; add_header Content-Type text/plain; }\n"
        "    location / { proxy_pass http://recommender_backend; proxy_set_header Host $host; }\n"
        "  }\n"
        "}\n"
    )
    tmp_file.flush()
    tmp_file.close()
    return Path(tmp_file.name)


def perform_staged_shift(target_color: str, previous_color: str, args, dry_run: bool) -> bool:
    if dry_run:
        print("(dry-run) Skipping staged shift.")
        return True

    lb_health_url = f"{args.lb_url}{args.health_path}"

    def guard(stage: str) -> bool:
        print(f"Validating before stage '{stage}' ...")
        if not wait_for_health(lb_health_url, args.health_attempts, args.health_interval, dry_run=False):
            print("Health check failed during staged shift.")
            return False
        if args.online_metrics_url and args.online_metric_min is not None:
            if not check_online_metric(args.online_metrics_url, args.online_metric_name, args.online_metric_min):
                print("Online metric below threshold during staged shift.")
                return False
        return True

    phases = [
        ("old-primary", previous_color, "Keeping previous color primary (new = 25%)"),
        ("equal", None, "Balancing traffic 50/50"),
        ("new-primary", target_color, "Making new color primary (≈75%)"),
        ("final", target_color, "Final check before optional 100% cutover"),
    ]

    for name, color, desc in phases:
        print(f"\n[Staged shift] {desc}")
        if not guard(name):
            print("Staged shift guard failed, reverting to previous primary.")
            subprocess.run(["bash", "scripts/switch-traffic.sh", previous_color], check=False)
            return False

        if name == "equal":
            subprocess.run(["docker", "cp", "nginx-equal.conf", "recommender-loadbalancer:/tmp/nginx.conf"], check=False)
            subprocess.run(["docker", "exec", "recommender-loadbalancer",
                            "sh", "-c", "cat /tmp/nginx.conf > /etc/nginx/nginx.conf"], check=False)
            subprocess.run(["docker", "exec", "recommender-loadbalancer", "rm", "-f", "/tmp/nginx.conf"], check=False)
            subprocess.run(["docker", "exec", "recommender-loadbalancer", "nginx", "-s", "reload"], check=False)
        else:
            subprocess.run(["bash", "scripts/switch-traffic.sh", color], check=False)

        print(f"Waiting {args.shift_wait}s before next stage...")
        time.sleep(args.shift_wait)

    if args.stop_old_on_success:
        print(f"Stopping {previous_color} container per --stop-old-on-success")
        subprocess.run(["docker", "stop", f"recommender-{previous_color}"], check=False)

    return True

# for data provenance
def sha256(path: str) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# for pipeline provenance
def get_git_sha(repo_root: Path) -> str:
    """Get current git commit SHA."""
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root
    ).decode().strip()

# for provenance log
def write_provenance(
    release_id: str,
    ratings_csv: str,
    repo_root: Path,
    output_dir: Optional[Path] = None
) -> Path:
    """Write provenance.json for a model release."""
    if output_dir is None:
        output_dir = repo_root / "models"
    output_dir.mkdir(parents=True, exist_ok=True)

    prov = {
        "model_version": release_id,
        "training_timestamp": datetime.utcnow().isoformat(),
        "pipeline_git_sha": get_git_sha(repo_root),
        "framework_versions": {
            "python": sys.version,
            "surprise": surprise.__version__,
        },
        "training_data": {
            "source": "kafka",
            "filename": "explicit_ratings_from_kafka.csv",  
            "server": "fall2025-comp585.cs.mcgill.ca:9092",  # or args.kafka_server
            "num_rows": sum(1 for _ in open(ratings_csv)) - 1,  # exclude header
            "csv_sha256": sha256(ratings_csv),
        }
    }

    prov_path = output_dir / f"{release_id}_provenance.json"
    prov_path.write_text(json.dumps(prov, indent=2))
    print(f"\n✅ Provenance written to {prov_path}")
    return prov_path

def main():
    parser = argparse.ArgumentParser(description="Automate retraining and deployment pipeline.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually run commands (otherwise dry-run).")
    parser.add_argument("--mode", default="stream")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip Kafka ingestion step.")
    parser.add_argument("--kafka-server", default="fall2025-comp585.cs.mcgill.ca:9092",
                        help="Kafka bootstrap server.")
    parser.add_argument("--team-number", type=int, default=2,
                        help="Team/topic identifier for Kafka.")
    parser.add_argument("--test-size", type=float, default=0.2,
                        help="Fraction of data used for the Surprise test split.")
    parser.add_argument("--k", type=int, default=20, help="Ranking cutoff for evaluation.")
    parser.add_argument("--threshold", type=float, default=3.5,
                        help="Relevance threshold for ranking metrics.")
    parser.add_argument("--rmse-max", type=float, default=1.0,
                        help="Abort if RMSE exceeds this value.")
    parser.add_argument("--precision-min", type=float, default=0.05,
                        help="Abort if precision@k falls below this value.")
    parser.add_argument("--recall-min", type=float, default=0.02,
                        help="Abort if recall@k falls below this value.")
    parser.add_argument("--hit-rate-min", type=float, default=0.1,
                        help="Abort if hit-rate@k falls below this value.")
    parser.add_argument("--mrr-min", type=float, default=0.05,
                        help="Abort if MRR falls below this value.")
    parser.add_argument("--health-path", default="/health")
    parser.add_argument("--health-attempts", type=int, default=10)
    parser.add_argument("--health-interval", type=float, default=6.0)
    parser.add_argument("--release-log", default=str(REPO_ROOT / "reports" / "release_history.jsonl"))
    parser.add_argument("--metrics-out", default=str(REPO_ROOT / "reports" / "latest_metrics.json"))
    parser.add_argument("--staged-shift", action="store_true", default=True,
                        help="Enable staged traffic shift checks after deploy (default: on).")
    parser.add_argument("--shift-wait", type=int, default=3600,
                        help="Seconds to wait between staged traffic shifts (default: 3600).")
    parser.add_argument("--lb-url", default="http://127.0.0.1:8082",
                        help="Base URL for load balancer health endpoint.")
    parser.add_argument("--online-metrics-url", default="http://127.0.0.1:9108/metrics",
                        help="HTTP endpoint exposing online metrics (Prometheus text).")
    parser.add_argument("--online-metric-name", default="online_positive_rating_rate",
                        help="Metric name to check before advancing staged shifts.")
    parser.add_argument("--online-metric-min", type=float, default=0.35,
                        help="Minimum acceptable value for the online metric (default: 0.35).")
    parser.add_argument("--stop-old-on-success", action="store_true", default=True,
                        help="Stop the previous color container after staged shift completes.")
    parser.add_argument("--release-log-path", default=str(REPO_ROOT / "reports" / "release_history.jsonl"))
    parser.add_argument("--metrics-out-path", default=str(REPO_ROOT / "reports" / "latest_metrics.json"))
    args = parser.parse_args()

    dry_run = not args.execute
    release_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    release_entry = {
        "release_id": release_id,
        "timestamp": datetime.utcnow().isoformat(),
        "image": None,
        "status": "pending",
        "metrics_file": args.metrics_out_path,
    }
    release_log_path = Path(args.release_log_path)

    try:
        ratings_csv = str(REPO_ROOT / "data" / "explicit_ratings_from_kafka.csv")
        max_rows = 100000
        ingest_mode = args.mode
        #"historical" #"stream"
        append_data = True
        if not args.skip_ingest:
            ingest_cmd = [
                PYTHON_BIN,
                str(REPO_ROOT / "scripts" / "extract_explicit_ratings.py"),
                "--mode", ingest_mode,
                "--kafka-server", args.kafka_server,
                "--team-number", str(args.team_number),
                "--output-file", ratings_csv,
                "--max-rows", str(max_rows),
            ]
            if append_data:
                ingest_cmd.append("--append")
            run_cmd(ingest_cmd, dry_run=dry_run)

        trainset_path = str(REPO_ROOT / "data" / "surprise_trainset.pkl")
        testset_path = str(REPO_ROOT / "data" / "surprise_testset.pkl")
        split_cmd = [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "comp585-recommender:pipeline",
            "python", "scripts/split_train_test_set.py",
            "--csv-path", "data/explicit_ratings_from_kafka.csv",
            "--trainset-output", "data/surprise_trainset.pkl",
            "--testset-output", "data/surprise_testset.pkl",
            "--test-size", str(args.test_size),
        ]
        run_cmd(split_cmd, dry_run=dry_run)

        train_cmd = [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "comp585-recommender:pipeline",
            "python", "scripts/train_surprise_models.py",
            "--model", "svd",
            "--output_model_path", "models/svd_model.pkl",
            "--trainset-path", "data/surprise_trainset.pkl",
            "--testset-path", "data/surprise_testset.pkl",
        ]
        run_cmd(train_cmd, dry_run=dry_run)

        # Write provenance for this release
        write_provenance(
            release_id=release_id,
            ratings_csv=ratings_csv,
            repo_root=REPO_ROOT,
        )

        eval_cmd = [
            "docker", "run", "--rm",
            "-v", f"{REPO_ROOT}:/app",
            "comp585-recommender:pipeline",
            "python", "scripts/offline_evaluation.py",
            "--model-path", "models/svd_model.pkl",
            "--trainset-path", "data/surprise_trainset.pkl",
            "--testset-path", "data/surprise_testset.pkl",
            "--k", str(args.k),
            "--threshold", str(args.threshold),
            "--metrics-out", args.metrics_out_path,
            "--rmse-max", str(args.rmse_max),
            "--precision-min", str(args.precision_min),
            "--recall-min", str(args.recall_min),
            "--hit-rate-min", str(args.hit_rate_min),
            "--mrr-min", str(args.mrr_min),
        ]
        run_cmd(eval_cmd, dry_run=dry_run)

        image_tag = f"comp585-recommender:{release_id}"
        release_entry["image"] = image_tag

        env = os.environ.copy()
        env["MODEL_VERSION"] = release_id
        bg_cmd = ["docker", "compose", "build", "recommender-blue", "recommender-green"]
        run_cmd(bg_cmd, dry_run=dry_run, cwd=REPO_ROOT)

        target_color = os.environ.get("BG_TARGET_COLOR", "green")
        previous_color = "blue" if target_color == "green" else "green"
        deploy_cmd = ["bash", "scripts/deploy-blue-green.sh", target_color, release_id]
        run_cmd(deploy_cmd, dry_run=dry_run, cwd=REPO_ROOT)

        initial_color = previous_color if args.staged_shift else target_color
        switch_cmd = ["bash", "scripts/switch-traffic.sh", initial_color]
        run_cmd(switch_cmd, dry_run=dry_run, cwd=REPO_ROOT)

        if args.staged_shift:
            ok = perform_staged_shift(target_color, previous_color, args, dry_run)
            if not ok:
                release_entry["status"] = "aborted"
                release_entry["reason"] = "Staged traffic shift failed"
                append_release_log(release_log_path, release_entry)
                raise SystemExit("Staged traffic shift failed; reverted to previous version.")

        release_entry["status"] = "deployed"
        append_release_log(release_log_path, release_entry)
        print("\n✅ Blue/green deployment complete.")

    except subprocess.CalledProcessError as exc:
        release_entry["status"] = "failed"
        release_entry["reason"] = f"Command failed: {' '.join(exc.cmd)}"
        append_release_log(release_log_path, release_entry)
        raise
    except Exception as exc:
        if release_entry.get("status") not in {"aborted", "failed"}:
            release_entry["status"] = "failed"
            release_entry["reason"] = str(exc)
            append_release_log(release_log_path, release_entry)
        raise


if __name__ == "__main__":
    main()