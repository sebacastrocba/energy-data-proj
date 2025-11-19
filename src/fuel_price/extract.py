# Funciones para extraer datos de APIs

import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Optional, List, Tuple
import requests
from fuel_price.config import START_DATE_BRENT, START_DATE_DOLLAR

# Funciones auxiliares


def get_project_root() -> Path:
    """Obtiene la ruta raíz del proyecto."""
    return Path(__file__).parent.parent.parent


def get_default_data_path() -> Path:
    """Obtiene la ruta por defecto al directorio de datos."""
    return get_project_root() / "data" / "raw"


def get_today_date() -> str:
    """Obtiene la fecha de hoy en formato YYYY-MM-DD."""
    return datetime.today().strftime("%Y-%m-%d")


def find_csv_files(data_path: Path, pattern: str = "*precios*.csv") -> List[Path]:
    """Busca archivos CSV en un directorio."""
    return list(data_path.glob(pattern))


def run_download_script(script_path: Path) -> None:
    """Ejecuta el script de descarga de datos."""
    print(f"Ejecutando script de descarga: {script_path.name}")
    subprocess.run(["python", str(script_path)], check=True)


##################################################################################################

# Funciones principales de extracción

###################################################################################
## PRECIO DEL BRENT
###################################################################################


def extract_brent_prices(
    start_date: str = START_DATE_BRENT,
    end_date: Optional[str] = None,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Extrae precios históricos de Brent.
    Siempre descarga desde start_date hasta hoy.
    Sobrescribe archivo existente para mantener datos actualizados.

    Args:
        start_date: Fecha de inicio (default: "2022-01-01")
        end_date: Fecha de fin (default: None = hoy)
        output_path: Dónde guardar (default: data/raw)

    Returns:
        DataFrame con datos completos actualizados
    """
    # Fecha de fin (por defecto: hoy)
    if end_date is None:
        end_date = get_today_date()

    if output_path is None:
        output_path = get_default_data_path()

    # Descargar desde start_date
    print(f"Descargando Brent desde {start_date} hasta {end_date}...")
    brent_data = yf.download("BZ=F", start=start_date, end=end_date, progress=False)

    # Validar
    if brent_data is None or brent_data.empty:
        raise ValueError(
            f"No se obtuvieron datos de Brent para el período {start_date} - {end_date}. "
            "Verifica tu conexión a internet."
        )

    # Procesar
    brent_df = brent_data["Close"].reset_index()
    brent_df.columns = ["date", "brent_price"]

    print(f"Descargados {len(brent_df):,} registros de Brent")

    # Sobrescribe archivo anterior
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / "brent_prices.csv"

    brent_df.to_csv(file_path, index=False)
    print(f"Archivo actualizado: {file_path}")
    print(f"   Período: {start_date} a {end_date}")
    print(f"   Registros: {len(brent_df):,}")

    return brent_df


##################################################################################
## PRECIO DE DOLAR OFICIAL Y BLUE
##################################################################################


def extract_dolar_bluelytics(
    start_date: str = START_DATE_DOLLAR,
    end_date: Optional[str] = None,
    tipos: List[str] = ["oficial", "blue"],
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Extrae cotización histórica USD/ARS desde Bluelytics API.

    API: https://bluelytics.com.ar/
    Endpoint: https://api.bluelytics.com.ar/v2/evolution.json

    Args:
        start_date: Fecha inicio (YYYY-MM-DD)
        end_date: Fecha fin (default: hoy)
        tipos: Lista de tipos ['oficial', 'blue']
        output_path: Dónde guardar CSV

    Returns:
        DataFrame con datos completos actualizados.
    """

    if end_date is None:
        end_date = get_today_date()

    print("\n" + "=" * 70)
    print("EXTRACCIÓN USD/ARS - BLUELYTICS API")
    print("=" * 70)
    print(f"Rango solicitado: {start_date} a {end_date}")
    print(f"Tipos: {', '.join(tipos)}")
    print(f"Fuente: api.bluelytics.com.ar")
    print("=" * 70 + "\n")

    # Descargar todos los datos históricos
    url = "https://api.bluelytics.com.ar/v2/evolution.json"
    print("Descargando datos históricos...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error al obtener datos de Bluelytics: {e}")

    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError("La API de Bluelytics devolvió un resultado vacío.")

    df["date"] = pd.to_datetime(df["date"])
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

    if tipos:
        tipos_normalizados = {tipo.lower() for tipo in tipos}
        df = df[df["source"].str.lower().isin(tipos_normalizados)]

    df = df.sort_values("date").reset_index(drop=True)

    print(f"\nDatos procesados: {len(df):,} registros")
    if not df.empty:
        for source, sub_df in df.groupby("source"):
            print(f"   {source} → {len(sub_df)} días")

    if output_path is None:
        output_path = get_default_data_path()

    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / "usd_ars_bluelytics.csv"
    df.to_csv(file_path, index=False)
    print(f"\nGuardado en: {file_path}")

    return df


##################################################################################
## PRECIOS DE COMBUSTIBLES
##################################################################################


def extract_fuel_prices(
    data_path: Optional[Path] = None, update_data: bool = True
) -> pd.DataFrame:
    """
    Extrae precios de combustibles.
    Ejecuta script de descarga si update_data=True.

    Args:
        data_path: Directorio de datos (default: data/raw)
        update_data: Si True, ejecuta descarga (default: True)

    Returns:
        DataFrame con datos completos de combustibles
    """
    if data_path is None:
        data_path = get_default_data_path()

    # Ejecutar script de descarga
    if update_data:
        print(f"Actualizando datos de combustibles...")

        project_root = data_path.parent.parent
        script_path = project_root / "src" / "fuel_price" / "get_price_data_SE.py"

        # El script decide automáticamente qué descargar según el cache
        run_download_script(script_path)
        print(f"Script completado - datos actualizados")
    else:
        print(f"Actualización omitida - leyendo archivos existentes")

    # Leer el archivo completo que ya está concatenado
    complete_file = data_path / "precios_eess_completo.csv"

    if not complete_file.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo completo: {complete_file}\n"
            "Ejecuta el script de descarga primero con update_data=True"
        )

    print(f"Leyendo archivo completo: {complete_file.name}")

    # Leer el archivo completo
    fuel_df = pd.read_csv(complete_file)

    print(f"Cargados {len(fuel_df):,} registros de combustibles")

    return fuel_df


###################################################################################
## EXTRACCIÓN COMPLETA DE DATOS
###################################################################################


def extract_all_data(
    brent_start_date: str = "2022-01-01",
    brent_end_date: Optional[str] = None,
    fuel_data_path: Optional[Path] = None,
    update_all: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extrae todas las fuentes de datos.

    Fuentes:
    1. Brent (Yahoo Finance) - Precios en USD
    2. Combustibles (Secretaría de Energía) - Precios en ARS
    3. USD/ARS (Bluelytics) - Cotización oficial + blue

    Args:
        brent_start_date: Fecha inicio Brent (default: "2022-01-01")
        brent_end_date: Fecha fin Brent (default: hoy)
        fuel_data_path: Path combustibles (default: data/raw)
        update_all: Si True, actualiza todo (default: True)

    Returns:
        Tupla (brent_df, fuel_df, dolar_df) con datos completos actualizados
    """
    print("\n" + "=" * 70)
    print("EXTRACCIÓN COMPLETA - 3 FUENTES DE DATOS")
    print("=" * 70)
    print(f"Fecha de actualización: {get_today_date()}")
    print("Fuentes:")
    print("  1. Brent (Yahoo Finance) → Precios petróleo en USD")
    print("  2. Combustibles (Sec. Energía ARG) → Precios en ARS")
    print("  3. USD/ARS (Bluelytics) → Cotización oficial + blue")
    print("=" * 70)
    print()

    # ========================================
    # 1. BRENT (USD)
    # ========================================
    print("1. EXTRAYENDO PRECIOS DE BRENT (USD)")
    print("-" * 70)
    brent_prices = extract_brent_prices(
        start_date=brent_start_date, end_date=brent_end_date, output_path=fuel_data_path
    )
    print()

    # ========================================
    # 2. COMBUSTIBLES (ARS)
    # ========================================
    print("2. EXTRAYENDO PRECIOS DE COMBUSTIBLES (ARS)")
    print("-" * 70)
    fuel_prices = extract_fuel_prices(data_path=fuel_data_path, update_data=update_all)
    print()

    # ========================================
    # 3. USD/ARS (OFICIAL + BLUE)
    # ========================================
    print("3. EXTRAYENDO COTIZACIÓN USD/ARS")
    print("-" * 70)
    dolar_data = extract_dolar_bluelytics(
        start_date=brent_start_date,
        end_date=brent_end_date,
        tipos=["oficial", "blue"],
        output_path=fuel_data_path,
    )
    print()

    # ========================================
    # RESUMEN FINAL
    # ========================================
    print("=" * 70)
    print("EXTRACCIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 70)
    print(f"Datos disponibles:")
    print(f"\n   BRENT (USD):")
    print(f"     - Registros: {len(brent_prices):,}")
    print(
        f"     - Período: {brent_prices['date'].min()} a {brent_prices['date'].max()}"
    )
    print(
        f"     - Precio promedio: ${brent_prices['brent_price'].mean():.2f} USD/barril"
    )

    print(f"\n   COMBUSTIBLES (ARS):")
    print(f"     - Registros: {len(fuel_prices):,}")
    # Mostrar info adicional si hay columnas conocidas
    if "producto" in fuel_prices.columns:
        print(f"     - Productos únicos: {fuel_prices['producto'].nunique()}")
    if "provincia" in fuel_prices.columns:
        print(f"     - Provincias únicas: {fuel_prices['provincia'].nunique()}")

    print(f"\n   USD/ARS (Oficial + Blue):")
    print(f"     - Registros: {len(dolar_data):,}")
    print(
        f"     - Período: {dolar_data['date'].min().date()} a {dolar_data['date'].max().date()}"
    )
    if "usd_ars_oficial" in dolar_data.columns:
        print(
            f"     - USD Oficial promedio: ${dolar_data['usd_ars_oficial'].mean():.2f}"
        )
    if "usd_ars_blue" in dolar_data.columns:
        print(f"     - USD Blue promedio: ${dolar_data['usd_ars_blue'].mean():.2f}")
    if "brecha_cambiaria_pct" in dolar_data.columns:
        print(
            f"     - Brecha cambiaria promedio: {dolar_data['brecha_cambiaria_pct'].mean():.2f}%"
        )

    print(
        f"\n   TOTAL REGISTROS: {len(brent_prices) + len(fuel_prices) + len(dolar_data):,}"
    )
    print(f"\nÚltima actualización: {get_today_date()}")
    print("=" * 70 + "\n")

    return brent_prices, fuel_prices, dolar_data


if __name__ == "__main__":
    # Ejecutar actualización completa
    brent, fuel, dolar = extract_all_data()

    print("\n" + "=" * 70)
    print("PREVIEW DE DATOS EXTRAÍDOS")
    print("=" * 70)

    print("\nBRENT (primeros 5 registros):")
    print(brent.head())

    print("\nCOMBUSTIBLES (primeros 5 registros):")
    print(fuel.head())

    print("\nUSD/ARS (primeros 5 registros):")
    print(dolar.head())

    print("\nUSD/ARS (últimos 5 registros):")
    print(dolar.tail())
