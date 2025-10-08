"""
Ejemplo básico de Regresión Logística
"""

import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.datasets import make_classification

# Generar datos sintéticos para clasificación binaria
X, y = make_classification(
    n_samples=1000,      # 1000 muestras
    n_features=2,        # 2 características (para visualización fácil)
    n_informative=2,     # 2 características informativas
    n_redundant=0,       # sin características redundantes
    n_clusters_per_class=1,
    random_state=42
)

# Dividir los datos en conjunto de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

# Crear y entrenar el modelo de regresión logística
modelo = LogisticRegression(random_state=42)
modelo.fit(X_train, y_train)

# Realizar predicciones
y_pred = modelo.predict(X_test)
y_pred_proba = modelo.predict_proba(X_test)

# Evaluar el modelo
accuracy = accuracy_score(y_test, y_pred)
conf_matrix = confusion_matrix(y_test, y_pred)

print(f"\nPrecisión del modelo: {accuracy:.4f}")
print(f"\nMatriz de confusión:\n{conf_matrix}")
print(f"\nReporte de clasificación:\n")
print(classification_report(y_test, y_pred))
