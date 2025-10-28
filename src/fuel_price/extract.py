# Funciones para extraer datos de APIs

import pandas as pd
import yfinance as yf
from datetime import datetime 
from pathlib import Path
import subprocess

def extract_brent_prices(start_date: str = "2022-01-01") -> pd.DataFrame:
    """
    Extrae los precios históricos del petróleo Brent desde Yahoo Finance.
    
    Args:
        start_date (str): Desde cuando queremos datos

    Returns:
        Una tabla con los precios históricos del petróleo Brent.

    """

    # Obtener fecha de hoy
    today = datetime.today().strftime("%Y-%m-%d")

    # Descargar datos de Yahoo Finance
    brent_data = yf.download("BZ=F", start=start_date, end=today, progress=False)

    # Quedarnos solo con el precio de cierre
    brent_df = brent_data['Close'].reset_index()

    # Renombrar columnas 
    brent_df.columns = ["date", "brent_price"]

    return brent_df

def extract_fuel_prices():
    """
    Extrae los precios de combustibles desde archivos CSV.
    
    Busca archivos CSV en la carpeta data/raw del proyecto y los combina.
    Si no existen, ejecuta el script de descarga primero.
    
    Returns:
        DataFrame con todos los precios de combustibles combinados.
    """
    
    # Obtener la ruta absoluta al directorio data/raw
    project_root = Path(__file__).parent.parent.parent
    data_path = project_root / "data" / "raw"
    
    # Buscar CSVs ya generados con el patrón de archivos descargados
    csv_files = list(data_path.glob("*precios*.csv"))
    
    # Si no existen, ejecutar conversor
    if not csv_files:
        print(f"No se encontraron archivos CSV en {data_path}")
        print("Ejecutando descarga de datos...")
        script_path = project_root / "src" / "pda_project" / "get_price_data_SE_linux.py"
        subprocess.run(["python", str(script_path)], check=True)
        csv_files = list(data_path.glob("*precios*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"No se pudieron encontrar archivos CSV en {data_path}")
    
    print(f"Archivos CSV encontrados: {len(csv_files)}")
    for csv_file in csv_files:
        print(f"  - {csv_file.name}")
    
    # Leer y combinar
    dfs = [pd.read_csv(f) for f in csv_files]
    return pd.concat(dfs, ignore_index=True)

def extract_all_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extrae datos de precios de Brent y precios de combustibles en Argentina.

    Returns:
        Una tupla con dos tablas: (brent_prices, fuel_prices)
    """

    brent_prices = extract_brent_prices()
    fuel_prices = extract_fuel_prices()

    return brent_prices, fuel_prices

if __name__ == "__main__":

    brent_prices, fuel_prices = extract_all_data()
    print("Brent Prices:")
    print(brent_prices.head())
    print("\nFuel Prices:")
    print(fuel_prices.head())





