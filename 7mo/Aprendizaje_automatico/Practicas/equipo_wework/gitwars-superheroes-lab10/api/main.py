from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

data = joblib.load("api/model/preprocessor.pkl")
mean_ = data["mean"]
std_ = data["std"]

class SuperheroPreprocessor:
    def transform(self, X):
        X = np.asarray(X, dtype=float)
        return (X - mean_) / std_

preprocessor = SuperheroPreprocessor()

app = FastAPI()

class Features(BaseModel):
    intelligence: float
    strength: float
    speed: float
    durability: float
    combat: float
    height_cm: float
    weight_kg: float

class PredictRequest(BaseModel):
    features: Features

# Cargar modelo y preprocessor una sola vez
params = joblib.load("api/model/best_svm_params.pkl")
model = joblib.load("api/model/best_trained_svm_model.pkl")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/info")
def info():
    return {
        "team": "wework",
        "model_type": "SVM",
        "best_params": { "C": params[0], "gamma": params[1] },                 
        "preprocessing": "Numerical standardization: (x - mean) / std using statistics from the cleaned 600-sample dataset."
    }

@app.post("/predict")
def predict(req: PredictRequest):
    f = req.features
    x = np.array([[f.intelligence, f.strength, f.speed,
                   f.durability, f.combat, f.height_cm, f.weight_kg]])
    x = preprocessor.transform(x)
    y_hat = model.predict(x)[0]
    return {"prediction": float(y_hat)}
