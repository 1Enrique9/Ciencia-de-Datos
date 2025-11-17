import requests
import pandas as pd
import numpy as np
import os
import re


BASE_URL = "https://akabab.github.io/superhero-api/api/all.json"


# Funciones para parsear altura y peso
def parse_height(height_list):
    if not height_list or len(height_list) == 0:
        return pd.NA

    h1 = str(height_list[0])
    h2 = str(height_list[1]) if len(height_list) > 1 else None

    # 1) Intentar usar cm
    if h2 and "cm" in h2 and h2 not in ["0 cm", "-", "null"]:
        try:
            return float(h2.replace("cm", "").strip())
        except:
            pass

    # 2) Convertir pies/pulgadas
    pattern = r"(\d+)'(\d+)"
    match = re.search(pattern, h1)
    if match:
        feet = int(match.group(1))
        inches = int(match.group(2))
        return round(feet * 30.48 + inches * 2.54, 2)

    return pd.NA


def parse_weight(weight_list):
    if not weight_list or len(weight_list) == 0:
        return pd.NA

    w1 = str(weight_list[0])
    w2 = str(weight_list[1]) if len(weight_list) > 1 else None

    # 1) Usar kg
    if w2 and "kg" in w2 and w2 not in ["0 kg", "-", "null"]:
        try:
            return float(w2.replace("kg", "").strip())
        except:
            pass

    # 2) Convertir libras → kg
    if "lb" in w1 and w1 not in ["- lb", "-lb"]:
        try:
            lbs = float(w1.replace("lb", "").replace("-", "").strip())
            return round(lbs * 0.453592, 2)
        except:
            pass

    return pd.NA


# Conseguir datos desde la API y procesarlos
def fetch_superhero_data():
    print("Descargando datos desde la API...")
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
    heroes = resp.json()

    # Crear DataFrame crudo
    df = pd.json_normalize(heroes)

    # Extraer columnas relevantes
    df_clean = pd.DataFrame({
        "intelligence": df["powerstats.intelligence"],
        "strength": df["powerstats.strength"],
        "speed": df["powerstats.speed"],
        "durability": df["powerstats.durability"],
        "combat": df["powerstats.combat"],
        "power": df["powerstats.power"],
        "height_cm": df["appearance.height"].apply(parse_height),
        "weight_kg": df["appearance.weight"].apply(parse_weight)
    })

    # Convertir todo a numérico
    df_clean = df_clean.apply(pd.to_numeric, errors="coerce")

    # Eliminar NA
    before = len(df_clean)
    df_clean = df_clean.dropna()
    after = len(df_clean)

    print(f"Filas antes de limpieza: {before}")
    print(f"Filas después de limpieza: {after}")

    # Asegurar al menos 600 registros
    TARGET = 600
    if len(df_clean) < TARGET:
        needed = TARGET - len(df_clean)
        print(f"Faltan {needed} registros → Generando registros sintéticos...")

        # Upsampling simple
        df_extra = df_clean.sample(needed, replace=True, random_state=42).copy()

        # Ruido suave (std=0.2)
        for col in df_extra.columns:
            noise = np.random.normal(0, 0.2, size=len(df_extra))
            df_extra[col] = df_extra[col] + noise

        # Redondear **solo df_extra** a 1 decimal
        df_extra = df_extra.round(1)

        # Combinar
        df_final = pd.concat([df_clean, df_extra], ignore_index=True)

    else:
        df_final = df_clean.sample(TARGET, random_state=42).reset_index(drop=True)

    # Guardar CSV
    os.makedirs("data", exist_ok=True)
    output_path = "./data/data.csv"
    df_final.to_csv(output_path, index=False)

    print("===================================")
    print("Archivo generado:", output_path)



if __name__ == "__main__":
    fetch_superhero_data()

