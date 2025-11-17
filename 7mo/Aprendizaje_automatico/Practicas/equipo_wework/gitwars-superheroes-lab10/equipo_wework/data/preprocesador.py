import numpy as np
import pandas as pd
import joblib
import os

df = pd.read_csv("data/data.csv")

cols = [
    "intelligence", "strength", "speed", "durability",
    "combat", "height_cm", "weight_kg"
]

X = df[cols].values.astype(float)

mean_ = X.mean(axis=0)
std_  = X.std(axis=0)
std_[std_ == 0] = 1.0

os.makedirs("model", exist_ok=True)
joblib.dump({"mean": mean_, "std": std_}, "api/model/preprocessor.pkl")

print("Preprocesador guardado en api/model/preprocessor.pkl")
