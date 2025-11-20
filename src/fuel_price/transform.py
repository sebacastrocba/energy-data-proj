# Funciones para transformar datos

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Callable
from functools import wraps
import time
import logging
from fuel_price.config import PRODUCTO_MAP, COLUMNAS_RELEVANTES, START_DATE_FUEL_PRICE

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def timer(func: Callable) -> Callable:
    """Mide tiempo de ejecución de la función"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"{func.__name__} - Tiempo: {elapsed:.2f}s")
        return result

    return wrapper


def save_to_parquet(
    df: pd.DataFrame,
    output_path: Path,
    filename: str,
    partition_cols: Optional[List[str]] = None,
) -> Path:
    """
    Guarda DataFrame en formato Parquet para staging.

    Args:
        df: DataFrame a guardar
        output_path: Directorio de salida
        filename: Nombre del archivo
        partition_cols: Columnas para particionar (opcional)

    Returns:
        Path al archivo guardado
    """
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / f"{filename}.parquet"

    df.to_parquet(
        file_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
        partition_cols=partition_cols,
    )

    logger.info(f"Datos guardados en: {file_path}")

    # Mostrar tamaño en unidad apropiada
    size_bytes = file_path.stat().st_size
    if size_bytes < 1024 * 1024:  # Menor a 1 MB
        size_kb = size_bytes / 1024
        logger.info(f"Tamaño: {size_kb:.2f} KB")
    else:
        size_mb = size_bytes / 1024 / 1024
        logger.info(f"Tamaño: {size_mb:.2f} MB")

    return file_path


#######################################################################################
# Transformaciones para datos de precios del Brent
########################################################################################


@timer
def clean_brent_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia datos de Brent leídos desde CSV.

    - Convierte date a datetime
    - Asegura que precio sea float
    - Renombra columna para consistencia
    """
    logger.info("Iniciando limpieza de Brent")
    cleaned_df = df.copy()

    # Conversión de tipos (necesario después de leer CSV)
    cleaned_df["date"] = pd.to_datetime(cleaned_df["date"])
    cleaned_df["brent_price"] = cleaned_df["brent_price"].astype(float)

    logger.info("Limpieza de Brent completada")

    return cleaned_df


@timer
def agg_brent_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos diarios de Brent a nivel mensual (promedio).
    """
    logger.info("Agregando datos de Brent - Frecuencia: mensual")
    df_agg = (
        df.set_index("date").resample("ME").agg({"brent_price": np.mean}).reset_index()
    )
    df_agg = df_agg.rename(columns={"brent_price": "avg_brent_price"})
    logger.info(f"Agregación completada - {len(df_agg):,} meses generados")

    return df_agg


@timer
def process_brent_price_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline para procesar y transformar datos del precio del Brent
    """
    logger.info("=" * 70)
    logger.info("INICIANDO PIPELINE DE BRENT")
    logger.info("=" * 70)

    # Paso 1: Limpiar
    df_cleaned = clean_brent_price(raw_df)

    # Paso 2: Transformar y agregar
    df_transformed = agg_brent_price(df_cleaned)

    # Paso 3: Guardar resultados
    project_root = Path(__file__).parent.parent.parent
    output_path = project_root / "data" / "processed"

    save_to_parquet(
        df_cleaned,
        output_path=output_path,
        filename="brent_price_cleaned",
    )

    save_to_parquet(
        df_transformed,
        output_path=output_path,
        filename="brent_price_monthly",
    )

    logger.info("Datos de Brent guardados en formato Parquet")

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE DE BRENT COMPLETADO")
    logger.info("=" * 70)

    return df_transformed


#######################################################################################
# Transformaciones para datos de precios de combustibles
########################################################################################


@timer
def clean_fuel_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia datos de precios de combustibles.

    Transformaciones aplicadas:
    - Normalización de nombres de columnas
    - Conversión de tipos de datos
    - Eliminación de nulos y duplicados
    - Mapeo de productos a nombres estándar

    Args:
        df: DataFrame crudo desde la API

    Returns:
        DataFrame limpio

    Raises:
        ValueError: Si faltan columnas requeridas
    """
    logger.info(
        f"Iniciando limpieza de combustibles - Registros iniciales: {len(df):,}"
    )

    cleaned_df = df.copy()

    # Normalizar columnas
    cleaned_df.columns = (
        cleaned_df.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "_")
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    logger.debug("  Columnas normalizadas")

    # Validar columnas requeridas
    required_cols = ["periodo", "precio_surtidor", "producto"]
    missing = set(required_cols) - set(cleaned_df.columns)
    if missing:
        logger.error(f"Faltan columnas requeridas: {missing}")
        logger.error(f"  Columnas disponibles: {list(cleaned_df.columns)}")
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    # Conversiones de tipos
    cleaned_df["periodo"] = pd.to_datetime(
        cleaned_df["periodo"], format="%Y/%m", errors="coerce"
    )
    cleaned_df["precio_surtidor"] = pd.to_numeric(
        cleaned_df["precio_surtidor"], errors="coerce"
    )

    # Filtrar por fecha mínima (después de convertir periodo a datetime)
    before_date_filter = len(cleaned_df)
    start_date = pd.to_datetime(START_DATE_FUEL_PRICE)
    cleaned_df = cleaned_df[cleaned_df["periodo"] >= start_date]
    date_filtered = before_date_filter - len(cleaned_df)
    if date_filtered > 0:
        logger.info(
            f"  Filtrados {date_filtered:,} registros anteriores a {START_DATE_FUEL_PRICE}"
        )
    min_date = cleaned_df["periodo"].min().date()
    max_date = cleaned_df["periodo"].max().date()
    logger.info(f"  Rango de fechas después del filtro: {min_date} a {max_date}")

    # Limpieza
    before_cleaning = len(cleaned_df)
    cleaned_df = cleaned_df.dropna(subset=["periodo", "precio_surtidor"])
    nulls_removed = before_cleaning - len(cleaned_df)
    if nulls_removed > 0:
        logger.info(
            f"  Eliminados {nulls_removed:,} registros con valores nulos en periodo/precio"
        )

    before_zero_filter = len(cleaned_df)
    cleaned_df = cleaned_df[cleaned_df["precio_surtidor"] >= 1.0]
    zeros_removed = before_zero_filter - len(cleaned_df)
    if zeros_removed > 0:
        logger.info(
            f"  Eliminados {zeros_removed:,} registros con precio_surtidor <= 0"
        )

    before_dedup = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    dupes_removed = before_dedup - len(cleaned_df)
    if dupes_removed > 0:
        logger.info(f"  Eliminados {dupes_removed:,} duplicados")

    cleaned_df = cleaned_df.sort_values("periodo").reset_index(drop=True)

    # Mapeo de productos
    productos_unicos_antes = cleaned_df["producto"].nunique()
    logger.info(f"  Productos únicos antes del mapeo: {productos_unicos_antes}")

    cleaned_df["producto"] = cleaned_df["producto"].str.lower().map(PRODUCTO_MAP)

    productos_sin_mapear = cleaned_df["producto"].isna().sum()
    if productos_sin_mapear > 0:
        logger.warning(
            f"  {productos_sin_mapear:,} registros con productos no mapeados (serán eliminados)"
        )

    cleaned_df = cleaned_df.dropna(subset=["producto"])

    productos_unicos_despues = cleaned_df["producto"].nunique()
    logger.info(f"  Productos únicos después del mapeo: {productos_unicos_despues}")
    logger.info(f"  Productos: {sorted(cleaned_df['producto'].unique())}")

    logger.info(f"Limpieza completada - Registros finales: {len(cleaned_df):,}")
    logger.info(
        f"  Rango: {cleaned_df['periodo'].min().date()} a {cleaned_df['periodo'].max().date()}"
    )

    return cleaned_df


@timer
def fuel_price_aggs(
    df: pd.DataFrame, columns_to_keep: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Agrega datos de combustibles.

    Args:
        df: DataFrame limpio
        columns_to_keep: Columnas a seleccionar (opcional)

    Returns:
        DataFrame agregado por periodo, provincia, bandera, producto
    """
    logger.info(f"Agregando datos de combustibles - Registros iniciales: {len(df):,}")

    if columns_to_keep is None:
        columns_to_keep = COLUMNAS_RELEVANTES

    # Verificar que las columnas existen
    missing_cols = set(columns_to_keep) - set(df.columns)
    if missing_cols:
        logger.warning(f"  Columnas faltantes (serán omitidas): {missing_cols}")
        columns_to_keep = [col for col in columns_to_keep if col in df.columns]

    df_selected = df[columns_to_keep].copy()

    df_aggregated = df_selected.groupby(["periodo", "producto"], as_index=False).agg(
        precio_surtidor_mediana=("precio_surtidor", "median"),
        volumen_total=("volumen", "sum"),
    )

    logger.info(
        f"Agregación completada - {len(df_aggregated):,} registros agregados a nivel nacional"
    )
    return df_aggregated


@timer
def process_fuel_data_pipeline(
    raw_df: pd.DataFrame, save_staging: bool = True
) -> pd.DataFrame:
    """
    Pipeline completo de transformación de combustibles.

    Args:
        raw_df: DataFrame crudo
        save_staging: Si True, guarda datos limpios en staging

    Returns:
        DataFrame transformado y agregado
    """
    logger.info("=" * 70)
    logger.info("INICIANDO PIPELINE DE COMBUSTIBLES")
    logger.info("=" * 70)

    # Paso 1: Limpiar
    logger.info("\nPASO 1: Limpieza de datos")
    cleaned_df = clean_fuel_price(raw_df)

    # Paso 2: Transformar
    logger.info("\nPASO 2: Agregación de datos")
    transformed_df = fuel_price_aggs(cleaned_df)

    # Paso 3: Guardar
    if save_staging:
        logger.info("\nPASO 3: Guardando datos limpios en staging")
        project_root = Path(__file__).parent.parent.parent
        output_path = project_root / "data" / "processed"

        save_to_parquet(
            cleaned_df,
            output_path=output_path,
            filename="fuel_price_cleaned",
        )

        save_to_parquet(
            transformed_df,
            output_path=output_path,
            filename="fuel_price_aggregated",
        )

        logger.info("Datos de combustibles guardados en formato Parquet")

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE DE COMBUSTIBLES COMPLETADO")
    logger.info("=" * 70)

    return transformed_df


#########################################################################################
# Funciones para datos de precios del dólar Blue y Oficial
#########################################################################################


@timer
def clean_dollar_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de precios del dólar Blue y Oficial.

    Args:
        df (pd.DataFrame): DataFrame con los datos originales.

    Returns:
        pd.DataFrame: DataFrame con la columna date convertida a datetime.
    """
    logger.info(
        f"Iniciando limpieza de datos de dolar Blue y Oficial - Registros iniciales: {len(df):,}"
    )

    df = df.copy()

    # Convertir tipo de dato de date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    logger.info(f"Limpieza completada - Registros finales: {len(df):,}")
    logger.info(f"  Rango: {df['date'].min().date()} a {df['date'].max().date()}")

    return df


@timer
def dollar_price_aggs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza agregaciones mensuales en el DataFrame de precios del dólar Blue y Oficial.

    Pivotea los datos para tener columnas separadas por tipo de cambio y calcula
    la brecha cambiaria.

    Args:
        df (pd.DataFrame): DataFrame con columnas ['date', 'source', 'value_buy', 'value_sell']

    Returns:
        pd.DataFrame: DataFrame con columnas ['date', 'usd_ars_oficial', 'usd_ars_blue', 'brecha_cambiaria_pct']
    """
    logger.info("Agregando datos de USD/ARS - Frecuencia: mensual")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Pivotear para separar Oficial y Blue
    # Usamos value_sell (precio de venta) que es el más relevante para el análisis
    df_pivot = df.pivot_table(
        index="date", columns="source", values="value_sell", aggfunc=np.mean
    )

    # Renombrar columnas (source tiene valores 'Oficial' y 'Blue')
    df_pivot.columns = [f"usd_ars_{col.lower()}" for col in df_pivot.columns]

    # Resamplear mensualmente
    df_monthly = df_pivot.resample("ME").mean().reset_index()

    # Calcular brecha cambiaria en porcentaje
    df_monthly["brecha_cambiaria_pct"] = (
        (df_monthly["usd_ars_blue"] - df_monthly["usd_ars_oficial"])
        / df_monthly["usd_ars_oficial"]
        * 100
    )

    logger.info(f"Agregación completada - {len(df_monthly):,} meses generados")
    logger.info(f"  Columnas: {list(df_monthly.columns)}")

    return df_monthly


@timer
def process_dolar_price_data(raw_df: pd.DataFrame):
    """
    Pipeline para limpiar y transformar datos del precio del dolar oficial y blue
    """

    logger.info("=" * 70)
    logger.info("INICIANDO PIPELINE DE DOLAR BLUE Y OFICIAL")
    logger.info("=" * 70)

    # Paso 1: Limpiar
    df_cleaned = clean_dollar_price(raw_df)

    # Paso 2: Transformar y agregar
    df_transformed = dollar_price_aggs(df_cleaned)

    # Paso 3: Guardar resultados
    project_root = Path(__file__).parent.parent.parent
    output_path = project_root / "data" / "processed"

    save_to_parquet(
        df_cleaned,
        output_path=output_path,
        filename="dollar_price_cleaned",
    )

    save_to_parquet(
        df_transformed,
        output_path=output_path,
        filename="dollar_price_aggregated",
    )

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE DE DOLAR COMPLETADO")
    logger.info("=" * 70)

    return df_transformed


# Probar las funciones de transformación
if __name__ == "__main__":
    from pathlib import Path

    # Obtener la ruta correcta al directorio de datos
    project_root = Path(__file__).parent.parent.parent
    data_path = project_root / "data" / "raw"

    print("=" * 70)
    print("PRUEBA DE PIPELINES DE TRANSFORMACIÓN")
    print("=" * 70)
    print(f"Directorio de datos: {data_path}")
    print("=" * 70)

    # 1. Pipeline de Brent
    print("\n[1/3] Ejecutando pipeline de Brent...")
    print("-" * 70)
    brent_file = data_path / "brent_prices.csv"
    if not brent_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {brent_file}")
    else:
        brent_raw = pd.read_csv(brent_file)
        brent_transformed = process_brent_price_data(brent_raw)
        print("\nResumen de datos transformados de Brent:")
        print(brent_transformed.head())
        print(f"Total de meses procesados: {len(brent_transformed)}")

    # 2. Pipeline de Dólar
    print("\n[2/3] Ejecutando pipeline de Dólar...")
    print("-" * 70)
    dollar_file = data_path / "usd_ars_bluelytics.csv"
    if not dollar_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {dollar_file}")
    else:
        dollar_raw = pd.read_csv(dollar_file)
        dollar_transformed = process_dolar_price_data(dollar_raw)
        print("\nResumen de datos transformados de Dólar:")
        print(dollar_transformed.head())
        print(f"Total de meses procesados: {len(dollar_transformed)}")

    # 3. Pipeline de Combustibles
    print("\n[3/3] Ejecutando pipeline de Combustibles...")
    print("-" * 70)
    fuel_file = data_path / "precios_eess_completo.csv"
    if not fuel_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {fuel_file}")
    else:
        fuel_raw = pd.read_csv(fuel_file)
        fuel_transformed = process_fuel_data_pipeline(fuel_raw, save_staging=True)
        print("\nResumen de datos transformados de Combustibles:")
        print(fuel_transformed.head())
        print(f"Total de registros agregados: {len(fuel_transformed)}")

    print("\n" + "=" * 70)
    print("TODOS LOS PIPELINES COMPLETADOS EXITOSAMENTE")
    print("=" * 70)
