from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from joblib import dump
import os
import pandas as pd

try:
    df = pd.read_csv(r"D:\iris_train_totalmente_limpio.csv")  
except FileNotFoundError:
    df = pd.read_csv("iris_train_totalmente_limpio.csv")

X = df.drop('target', axis=1).values
y = df['target'].values

# Dividir datos en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestClassifier(
    n_estimators=100,
    max_depth=5,
    random_state=42
)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
acc = accuracy_score(y_test, y_pred)
print(f"Precisión del modelo: {acc:.3f}")
