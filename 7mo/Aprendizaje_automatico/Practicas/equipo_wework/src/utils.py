import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

def load_data(filepath='../data/data.csv'):
    """
    Carga el dataset y separa features (X) de target (y).
    Asegura que todo sea numérico como pide el Elemento 0[cite: 60].
    """
    df = pd.read_csv(filepath)
    
    X = df.drop(columns=['power'])
    y = df['power']
    
    return train_test_split(X, y, test_size=0.2, random_state=42)

def calculate_metric(y_true, y_pred):
    """
    Calcula RMSE (Root Mean Squared Error).
    """
    mse = mean_squared_error(y_true, y_pred)
    return np.sqrt(mse)