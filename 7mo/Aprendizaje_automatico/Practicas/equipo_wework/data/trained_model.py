import pickle
import numpy as np
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
import pandas as pd
from sklearn.model_selection import train_test_split

# Re-define the _load_data function as it's a helper and not directly exposed
def _load_data():
    """
    Carga data/data.csv y regresa el split de entrenamiento y prueba.
    """
    df = pd.read_csv("data/data.csv")

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

# Load the training data
X_train, _, y_train, _ = _load_data()

# Best parameters identified from the previous analysis (SVM RS)
best_model_name = 'SVM'
best_C = 100.0
best_gamma = 0.01

print(f"Training the best model: {best_model_name} with C={best_C}, gamma={best_gamma}")

# Train the SVR model with the best hyperparameters
best_svm_model = SVR(C=best_C, gamma=best_gamma, kernel="rbf")
best_svm_model.fit(X_train, y_train)

# Define the filename for the trained model
output_model_filename = 'api/model/best_trained_svm_model.pkl'

# Save the trained model to a pickle file
with open(output_model_filename, 'wb') as f:
    pickle.dump(best_svm_model, f)

print(f"Best trained SVM model saved successfully to: {output_model_filename}")

# (Optional) Load the model back to verify
with open(output_model_filename, 'rb') as f:
    loaded_model = pickle.load(f)
print(f"Model loaded back successfully: {loaded_model}")