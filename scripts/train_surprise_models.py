import argparse
import os
import pickle
from pathlib import Path

from surprise import SVD, KNNBasic

def load_train_test_set(testset_path, trainset_path):

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


def train_svd(testset, trainset, save_path):
	# SVD (Matrix Factorization)
	svd = SVD()
	svd.fit(trainset)
	#predictions_svd = svd.test(testset)

	with open(save_path, 'wb') as f:
		pickle.dump(svd, f)

	print(f"INFO: SVD model trained and saved to {save_path}.")

def train_knn(testset, trainset, save_path):
	# Item-based CF
	sim_options = {'name': 'cosine', 'user_based': False}   # for collaborative filtering
	knn_item = KNNBasic(sim_options=sim_options)
	knn_item.fit(trainset)
	#predictions_knn = knn_item.test(testset)

	with open(save_path, 'wb') as f:
		pickle.dump(knn_item, f)
	print(f"INFO: KNN model trained and saved to {save_path}.")


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Train a SVD or KNN recommendation model.")

	parser.add_argument('--model', type=str, choices=['svd', 'knn'], required=True,
						help="Specify the model to train: 'svd' or 'knn'.")
	parser.add_argument('--output_model_path', type=str, required=True,
						help="Path to save the trained model (e.g., 'models/svd_model.pkl').")
	parser.add_argument('--trainset-path', type=str,
						default=str(Path(__file__).resolve().parent.parent / "data" / "surprise_trainset.pkl"),
						help="Path to the Surprise trainset pickle.")
	parser.add_argument('--testset-path', type=str,
						default=str(Path(__file__).resolve().parent.parent / "data" / "surprise_testset.pkl"),
						help="Path to the Surprise testset pickle.")

	args = parser.parse_args()

	trainset_path = args.trainset_path
	testset_path = args.testset_path

	# Load the trainset and testset
	trainset, testset = load_train_test_set(trainset_path, testset_path)

	if trainset is None or testset is None:
		print("ERROR: Failed to load trainset or testset. Exiting.")
		exit(1) # Exit with an error code

	# Train the selected model
	if args.model == 'svd':
		train_svd(trainset, testset, args.output_model_path)
	elif args.model == 'knn':
		train_knn(trainset, testset, args.output_model_path)
	else:
		print(f"ERROR: Unknown model type '{args.model}'. This should not happen due to argparse choices.")
		exit(1)












