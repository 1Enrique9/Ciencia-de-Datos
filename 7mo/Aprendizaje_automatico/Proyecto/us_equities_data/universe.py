import pandas as pd

def load_tickers_from_csv(path: str, column: str = "Symbol") -> list[str]:
    df = pd.read_csv(path)
    if column not in df.columns:
        raise ValueError(f"La columna '{column}' no existe en el CSV.")
    return (
        df[column]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )

def get_current_us_tickers_approx() -> list[str]:
    """
    Descarga listas actuales de tickers de NASDAQ, NYSE y AMEX
    usando las fuentes públicas oficiales.
    """
    tables = []

    urls = [
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ]

    for url in urls:
        try:
            df = pd.read_csv(url, sep="|", dtype=str)
            tables.append(df)
        except Exception as e:
            print(f"No se pudo obtener {url}: {e}")

    if not tables:
        raise RuntimeError("No se pudo obtener ninguna lista de tickers.")

    all_df = pd.concat(tables, ignore_index=True)

    sym_col = "Symbol" if "Symbol" in all_df.columns else "ACT Symbol"

    tickers = (
        all_df[sym_col]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )

    # Quitar símbolos raros: solo letras y números
    tickers = [t for t in tickers if t.isalnum()]

    return sorted(tickers)