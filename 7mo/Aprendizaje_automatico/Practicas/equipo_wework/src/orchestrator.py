import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor

# -----------------------------------------------------------
# Función auxiliar para carga de datos
# -----------------------------------------------------------

def _load_data():
    """
    Carga data/data.csv y regresa el split de entrenamiento y prueba.
    """
    df = pd.read_csv("../data/data.csv")

    # columnas obligatorias
    feature_cols = [
        "intelligence", "strength", "speed", 
        "durability", "combat", 
        "height_cm", "weight_kg"
    ]
    target_col = "power"

    X = df[feature_cols].values
    y = df[target_col].values

    # split consistente para todos los modelos
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    return X_train, X_test, y_train, y_test

# -----------------------------------------------------------
# SVM
# -----------------------------------------------------------

def evaluate_svm(C, gamma):
    """
    Entrena un SVR con hiperparámetros C y gamma.
    Retorna RMSE (float).
    """
    X_train, X_test, y_train, y_test = _load_data()

    model = SVR(C=C, gamma=gamma, kernel="rbf")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)

    return float(rmse)

# -----------------------------------------------------------
# Random Forest
# -----------------------------------------------------------

def evaluate_rf(n_estimators, max_depth):
    """
    Entrena un Random Forest Regressor con los hiperparámetros dados.
    Retorna RMSE (float).
    """
    X_train, X_test, y_train, y_test = _load_data()

    model = RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=42
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    return float(rmse)

# -----------------------------------------------------------
# MLP
# -----------------------------------------------------------
def evaluate_mlp(hidden_layer_sizes, alpha):
    """
    Entrena un MLPRegressor con los hiperparámetros dados.
    Retorna RMSE (float).
    """
    X_train, X_test, y_train, y_test = _load_data()

    model = MLPRegressor(
        hidden_layer_sizes=hidden_layer_sizes,
        alpha=alpha,
        max_iter=500,
        random_state=42
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)

    return float(rmse)



