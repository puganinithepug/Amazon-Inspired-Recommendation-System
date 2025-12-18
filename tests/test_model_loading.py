import pytest
import pandas as pd
import pickle
import os
from pathlib import Path

def test_model_file_exists():
    # Test that the model file exists in the models directory
    assert os.path.exists('models/svd_model.pkl')

def test_model_file_loads():
    # Test that the model can be loaded with pickle
    with open('models/svd_model.pkl','rb') as f:
        model = pickle.load(f)
    assert model is not None

def test_model_file_predict_method():
    # Test that the model has predict method
    with open('models/svd_model.pkl','rb') as f:
        model = pickle.load(f)
    assert hasattr(model, 'predict')

def test_model_file_trainset():
    # Test that the model has trainset attribute
    with open('models/svd_model.pkl','rb') as f:
        model = pickle.load(f)
    assert hasattr(model, 'trainset')

def test_model_file_can_predict():
    # Test that the model can make predictions for user-movie pairs
    with open('models/svd_model.pkl','rb') as f:
        model = pickle.load(f)
    pred = model.predict(1,'test_movie')
    assert pred is not None

def test_model_file_predict_est():
    # Test that the predictions have est attribute
    with open('models/svd_model.pkl','rb') as f:
        model = pickle.load(f)
    pred = model.predict(1,'test_movie')
    assert hasattr(pred, 'est')
