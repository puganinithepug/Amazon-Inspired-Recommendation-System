import argparse
import json
import os
import pickle
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from surprise import accuracy

# ---------- Helper Functions ----------
def load_model(model_path):
	if not os.path.exists(model_path):
		print(f"ERROR: Model file not found at '{model_path}'.")
		return None

	with open(model_path, 'rb') as f:
		model = pickle.load(f)
	
	return model

def load_train_test_set(trainset_path, testset_path):

	loaded_trainset = None
	loaded_testset = None

	if not os.path.exists(trainset_path) or not os.path.exists(testset_path):
		print(f"ERROR: Trainset file not found at '{trainset_path}'/'{testset_path}'.")
		return None, None

	with open(trainset_path, 'rb') as f:
		loaded_trainset = pickle.load(f)
	print("INFO: Trainset loaded successfully.")

	with open(testset_path, 'rb') as f:
		loaded_testset = pickle.load(f)
	print("INFO: Testset loaded successfully.")

	return loaded_trainset, loaded_testset

# ---------- Evaluation Metrics ----------

def evaluate_predicted_ratings(predictions, trainset, name="SVD"):
    """
    Evaluate predictions from Surprise and report RMSE/MAE,
    along with the proportion of new users in the test set.
    
    Parameters
    ----------
    predictions : list of Prediction objects
        The predictions returned by algo.test(testset)
    trainset : surprise.Trainset
        The training set used for fitting the model
    name : str
        A label for the model
    """
    # Identify which users were in the training data
    train_users = set(trainset._raw2inner_id_users.keys())
    
    # Determine how many predictions are for unseen users
    total_preds = len(predictions)
    new_user_preds = [p for p in predictions if p.uid not in train_users]
    known_user_preds = [p for p in predictions if p.uid in train_users]
    pct_new = len(new_user_preds) / total_preds * 100
    
    # Compute metrics overall
    rmse_all = accuracy.rmse(predictions, verbose=False)
    mae_all = accuracy.mae(predictions, verbose=False)
    
    # Optionally compute for known/new users separately
    rmse_known = accuracy.rmse(known_user_preds, verbose=False) if known_user_preds else None
    rmse_new = accuracy.rmse(new_user_preds, verbose=False) if new_user_preds else None
    
    print(f"=== {name} Evaluation ===")
    print(f"Total predictions: {total_preds}")
    print(f"New user predictions: {len(new_user_preds)} ({pct_new:.2f}%)")
    print(f"Known user predictions: {len(known_user_preds)} ({100 - pct_new:.2f}%)")
    print(f"RMSE (all): {rmse_all:.4f}, MAE (all): {mae_all:.4f}")
    if rmse_known is not None:
        print(f"RMSE (known users): {rmse_known:.4f}")
    if rmse_new is not None:
        print(f"RMSE (new users): {rmse_new:.4f}")
    print()
    
    return {
        "rmse_all": rmse_all,
        "mae_all": mae_all,
        "rmse_known": rmse_known,
        "rmse_new": rmse_new,
        "pct_new_users": pct_new
    }

def precision_recall_at_k(predictions, k=10, threshold=3.5, trainset=None):
    """
    Compute precision@k and recall@k. If trainset is provided,
    separates results for known vs new users.
    """
    # Group predictions by user
    user_est_true = defaultdict(list)
    for uid, _, true_r, est, _ in predictions:
        user_est_true[uid].append((est, true_r))

    precisions, recalls = {}, {}
    for uid, user_ratings in user_est_true.items():
        # Sort predictions by estimated rating (descending)
        user_ratings.sort(key=lambda x: x[0], reverse=True)
        
        # Mark which items are relevant (true rating ≥ threshold)
        rel_and_rec = [(true_r >= threshold) for (_, true_r) in user_ratings]
        
        n_rel = sum(rel_and_rec)       # number of relevant items
        n_rec_k = sum(rel_and_rec[:k]) # number of relevant items in top-k
        
        precisions[uid] = n_rec_k / k
        recalls[uid] = n_rec_k / n_rel if n_rel != 0 else 0

    # If trainset is provided, report new vs known separately
    if trainset is not None:
        train_users = set(trainset._raw2inner_id_users.keys())
        known_uids = [u for u in precisions if u in train_users]
        new_uids = [u for u in precisions if u not in train_users]

        # Compute means
        mean_prec_known = sum(precisions[u] for u in known_uids) / len(known_uids) if known_uids else None
        mean_rec_known = sum(recalls[u] for u in known_uids) / len(known_uids) if known_uids else None
        
        mean_prec_new = sum(precisions[u] for u in new_uids) / len(new_uids) if new_uids else None
        mean_rec_new = sum(recalls[u] for u in new_uids) / len(new_uids) if new_uids else None

        # Print results
        # if mean_prec_known is not None:
        #     print(f"Precision@{k} (known users): {mean_prec_known:.4f}")
        #     print(f"Recall@{k} (known users): {mean_rec_known:.4f}")
        # if mean_prec_new is not None:
        #     print(f"Precision@{k} (new users): {mean_prec_new:.4f}")
        #     print(f"Recall@{k} (new users): {mean_rec_new:.4f}")

    # Compute overall averages
    mean_prec = sum(precisions.values()) / len(precisions)
    mean_rec = sum(recalls.values()) / len(recalls)
    #print(f"Overall Precision@{k}: {mean_prec:.4f}, Recall@{k}: {mean_rec:.4f}")
    
    return precisions, recalls

def mean_reciprocal_rank(predictions, threshold=4.0):
    """Compute Mean Reciprocal Rank (MRR)."""
    user_est_true = defaultdict(list)
    for uid, _, true_r, est, _ in predictions:
        user_est_true[uid].append((est, true_r))

    rr_total, count = 0, 0
    for uid, user_ratings in user_est_true.items():
        user_ratings.sort(key=lambda x: x[0], reverse=True)
        for rank, (est, true_r) in enumerate(user_ratings, start=1):
            if true_r >= threshold:
                rr_total += 1.0 / rank
                break
        count += 1
    return rr_total / count if count else 0

def hit_rate_at_k(predictions, k=10, threshold=4.0, show_ratings=False):
    """
    Compute Hit Rate (HR@K).
    A "hit" occurs if at least one relevant item (true rating >= threshold)
    appears in the top-K predictions for a user.
    """
    user_est_true = defaultdict(list)
    for uid, _, true_r, est, _ in predictions:
        user_est_true[uid].append((est, true_r))

    hits = 0
    for uid, user_ratings in user_est_true.items():
        user_ratings.sort(key=lambda x: x[0], reverse=True)
        top_k = user_ratings[:k]

        # Print ratings for this user
        if show_ratings:
            print(f"\nUser {uid}:")
            for rank, (est, true_r) in enumerate(user_ratings, start=1):
                print(f"  Rank {rank:2d}: predicted = {est:.3f}, true = {true_r:.3f}")


        if any(true_r >= threshold for (_, true_r) in top_k):
            hits += 1

    return hits / len(user_est_true) if user_est_true else 0


def run_full_evaluation(model_path, trainset_path, testset_path, k=20, threshold=3.5):
    model = load_model(model_path)
    if model is None:
        raise FileNotFoundError(model_path)

    trainset, testset = load_train_test_set(trainset_path, testset_path)
    if trainset is None or testset is None:
        raise FileNotFoundError("Trainset or testset missing.")

    predictions = model.test(testset)
    rating_metrics = evaluate_predicted_ratings(predictions, trainset, "SVD")

    precisions, recalls = precision_recall_at_k(
        predictions, k=k, threshold=threshold, trainset=trainset
    )
    mean_precision = sum(precisions.values()) / len(precisions) if precisions else 0.0
    mean_recall = sum(recalls.values()) / len(recalls) if recalls else 0.0
    mrr = mean_reciprocal_rank(predictions, threshold=threshold)
    hr = hit_rate_at_k(predictions, k=k, threshold=threshold)

    metrics = {
        **rating_metrics,
        "precision_at_k": mean_precision,
        "recall_at_k": mean_recall,
        "hit_rate_at_k": hr,
        "mrr": mrr,
        "k": k,
        "relevance_threshold": threshold,
        "generated_at": datetime.utcnow().isoformat(),
    }
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Run offline evaluation and emit metrics.")
    parser.add_argument("--model-path", default="models/svd_model.pkl",
                        help="Path to the trained model pickle.")
    parser.add_argument("--trainset-path", default="data/surprise_trainset.pkl",
                        help="Path to Surprise trainset pickle.")
    parser.add_argument("--testset-path", default="data/surprise_testset.pkl",
                        help="Path to Surprise testset pickle.")
    parser.add_argument("--k", type=int, default=20, help="Cutoff for ranking metrics.")
    parser.add_argument("--threshold", type=float, default=3.5,
                        help="Relevance threshold for ranking metrics.")
    parser.add_argument("--metrics-out", help="Optional path to store metrics JSON.")
    parser.add_argument("--rmse-max", type=float, default=None,
                        help="Maximum acceptable RMSE. Abort if exceeded.")
    parser.add_argument("--precision-min", type=float, default=None,
                        help="Minimum acceptable precision@k.")
    parser.add_argument("--recall-min", type=float, default=None,
                        help="Minimum acceptable recall@k.")
    parser.add_argument("--hit-rate-min", type=float, default=None,
                        help="Minimum acceptable hit rate@k.")
    parser.add_argument("--mrr-min", type=float, default=None,
                        help="Minimum acceptable MRR.")
    args = parser.parse_args()

    metrics = run_full_evaluation(
        args.model_path, args.trainset_path, args.testset_path, k=args.k, threshold=args.threshold
    )

    print("\n=== Offline Evaluation Summary ===")
    for key in ["rmse_all", "mae_all", "precision_at_k", "recall_at_k", "hit_rate_at_k", "mrr"]:
        if key in metrics:
            print(f"{key}: {metrics[key]:.4f}")

    # if args.metrics_out:
    #     metrics_path = Path(args.metrics_out)
    #     metrics_path.parent.mkdir(parents=True, exist_ok=True)
    #     metrics_path.write_text(json.dumps(metrics, indent=2))
    #     print(f"\nMetrics written to {metrics_path}")

    if args.metrics_out:
        metrics_path = Path(args.metrics_out)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        
        with metrics_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(metrics) + "\n")
        print(f"\nMetrics appended to {metrics_path}")

    failures = []
    if args.rmse_max is not None and metrics["rmse_all"] > args.rmse_max:
        failures.append(f"RMSE {metrics['rmse_all']:.4f} > {args.rmse_max}")
    if args.precision_min is not None and metrics["precision_at_k"] < args.precision_min:
        failures.append(f"Precision@{args.k} {metrics['precision_at_k']:.4f} < {args.precision_min}")
    if args.recall_min is not None and metrics["recall_at_k"] < args.recall_min:
        failures.append(f"Recall@{args.k} {metrics['recall_at_k']:.4f} < {args.recall_min}")
    if args.hit_rate_min is not None and metrics["hit_rate_at_k"] < args.hit_rate_min:
        failures.append(f"HitRate@{args.k} {metrics['hit_rate_at_k']:.4f} < {args.hit_rate_min}")
    if args.mrr_min is not None and metrics["mrr"] < args.mrr_min:
        failures.append(f"MRR {metrics['mrr']:.4f} < {args.mrr_min}")

    if failures:
        print("\n❌ Evaluation failed thresholds:")
        for reason in failures:
            print(f"- {reason}")
        raise SystemExit(2)

    print("\n✅ Evaluation passed all thresholds.")


if __name__ == "__main__":
    main()
