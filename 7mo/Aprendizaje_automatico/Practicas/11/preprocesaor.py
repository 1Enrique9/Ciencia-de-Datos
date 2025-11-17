import re
import os
import pickle
import numpy as np
import pandas as pd

class SuperheroPreprocessor:
    """
    Preprocesador profesional:
    - Convierte height/weight a métricos
    - Detecta valores inválidos
    - Imputa NA con KNN (k=3)
    - Redondea a 1 decimal
    """

    def __init__(self, reference_df):
        """
        reference_df = dataframe limpio de 600 registros
        """
        self.ref = reference_df.copy()

    #  Conversiones métricas
    def parse_height(self, value):
        if value is None:
            return np.nan
        value = str(value).strip()

        if value in ["0 cm", "-", "null", "", "nan"]:
            return np.nan

        if "cm" in value:
            try:
                return float(value.replace("cm", "").strip())
            except:
                return np.nan

        pattern = r"(\d+)'(\d+)"
        m = re.search(pattern, value)
        if m:
            feet = int(m.group(1))
            inches = int(m.group(2))
            return round(feet * 30.48 + inches * 2.54, 2)

        return np.nan

    def parse_weight(self, value):
        if value is None:
            return np.nan
        value = str(value).strip()

        if value in ["0 kg", "- lb", "-", "null", "", "nan"]:
            return np.nan

        if "kg" in value:
            try:
                return float(value.replace("kg", "").strip())
            except:
                return np.nan

        if "lb" in value:
            try:
                lbs = float(value.replace("lb", "").strip())
                return round(lbs * 0.453592, 2)
            except:
                return np.nan

        return np.nan

    
    # Imputación con KNN
    
    def impute_knn(self, row, target_col, k=3):
        """
        row: DataFrame(1xN)
        target_col: 'height_cm' o 'weight_kg'
        """
        df = self.ref.copy()

        # columnas para distancia (todas excepto target)
        dist_cols = [c for c in df.columns if c != target_col]

        # fila del usuario
        x = row[dist_cols].values.astype(float).flatten()

        # matriz de referencia
        R = df[dist_cols].values.astype(float)

        # distancias euclidianas
        dist = np.sqrt(np.sum((R - x) ** 2, axis=1))

        # indices de los k vecinos más cercanos
        idx = dist.argsort()[:k]

        # valor imputado
        val = df.iloc[idx][target_col].mean()

        return round(val, 1)

 
    # Transformación final
    
    def transform(self, features):
        """
        Recibe dict → regresa vector listo.
        Correcciones + imputación incluida.
        """

        data = {
            "intelligence": float(features.get("intelligence", np.nan)),
            "strength": float(features.get("strength", np.nan)),
            "speed": float(features.get("speed", np.nan)),
            "durability": float(features.get("durability", np.nan)),
            "combat": float(features.get("combat", np.nan)),
            "power": np.nan,  # no viene en input
            "height_cm": self.parse_height(features.get("height") 
                                           or features.get("height_cm")),
            "weight_kg": self.parse_weight(features.get("weight_kg") 
                                           or features.get("weight"))
        }

        df = pd.DataFrame([data])

        # IMPUTAR height
        if pd.isna(df.loc[0, "height_cm"]):
            df.loc[0, "height_cm"] = self.impute_knn(df, "height_cm")

        # IMPUTAR weight
        if pd.isna(df.loc[0, "weight_kg"]):
            df.loc[0, "weight_kg"] = self.impute_knn(df, "weight_kg")

        # redondear final a 1 decimal
        df = df.round(1)

        # quitar power porque es target
        df = df.drop(columns=["power"])

        return df.values.reshape(1, -1)


# Guardar el preprocesador
if __name__ == "__main__":
    # Cargamos dataset limpio (600 filas)
    reference_df = pd.read_csv("data/data.csv")

    os.makedirs("models", exist_ok=True)
    preprocessor = SuperheroPreprocessor(reference_df)

    with open("models/preprocessor.pkl", "wb") as f:
        pickle.dump(preprocessor, f)

    print("Preprocesador guardado en models/preprocessor.pkl ")
