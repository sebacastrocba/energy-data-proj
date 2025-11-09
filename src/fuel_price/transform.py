# Funciones para transformar datos

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Callable
from functools import wraps
import time
import logging

if __name__ == "__main__":
    from config import PRODUCTO_MAP, COLUMNAS_RELEVANTES
else:
    from .config import PRODUCTO_MAP, COLUMNAS_RELEVANTES

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


#######################################################################################
# Transformaciones para datos de precios del Brent
########################################################################################


@timer
def clean_brent_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de precios del petróleo Brent.

    Args:
        df (pd.DataFrame): DataFrame con los datos originales.

    Returns:
        pd.DataFrame: DataFrame limpio y transformado.
    """
    logger.info(f"Iniciando limpieza de Brent - Registros iniciales: {len(df):,}")

    # Copiar el DataFrame para no modificar el original
    cleaned_df = df.copy()
    initial_count = len(cleaned_df)

    # 1. Eliminar filas con valores nulos
    cleaned_df = cleaned_df.dropna()
    nulls_removed = initial_count - len(cleaned_df)
    if nulls_removed > 0:
        logger.info(f"  Eliminados {nulls_removed:,} registros con valores nulos")

    # 2. Asegurarse de que la columna 'date' sea de tipo datetime (o renombrar si es necesario)
    if "Date" in cleaned_df.columns:
        cleaned_df = cleaned_df.rename(columns={"Date": "date"})
        logger.debug("  Columna 'Date' renombrada a 'date'")
    cleaned_df["date"] = pd.to_datetime(cleaned_df["date"])

    # 3. Asegurarse de que la columna de precio sea de tipo float (renombrar si es necesario)
    if "brent_price_usd" in cleaned_df.columns:
        cleaned_df = cleaned_df.rename(columns={"brent_price_usd": "brent_price"})
        logger.debug("  Columna 'brent_price_usd' renombrada a 'brent_price'")
    cleaned_df["brent_price"] = cleaned_df["brent_price"].astype(float)

    # 4. Eliminar duplicados
    before_dedup = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=["date"])
    dupes_removed = before_dedup - len(cleaned_df)
    if dupes_removed > 0:
        logger.info(f"  Eliminados {dupes_removed:,} duplicados")

    # 5. Ordenar por fecha
    cleaned_df = cleaned_df.sort_values(by="date").reset_index(drop=True)

    logger.info(f"Limpieza completada - Registros finales: {len(cleaned_df):,}")
    logger.info(
        f"  Rango: {cleaned_df['date'].min().date()} a {cleaned_df['date'].max().date()}"
    )

    return cleaned_df


@timer
def agg_brent_price(
    df: pd.DataFrame, freq: str = "M", agg_func: Callable = np.mean
) -> pd.DataFrame:
    """
    Realiza agregaciones en el DataFrame de precios del petróleo Brent.

    Args:
        df (pd.DataFrame): DataFrame con los datos originales.
        freq (str): Frecuencia de agregación ('M' para mensual, 'Q' para trimestral, etc.).
        agg_func (Callable): Función de agregación (por defecto np.mean).

    Returns:
        pd.DataFrame: DataFrame con las agregaciones realizadas.
    """
    freq_map = {
        "D": "diaria",
        "W": "semanal",
        "M": "mensual",
        "Q": "trimestral",
        "Y": "anual",
    }
    freq_name = freq_map.get(freq, freq)

    logger.info(f"Agregando datos de Brent - Frecuencia: {freq_name}")

    # Hacer una copia para no modificar el DataFrame original
    df = df.copy()

    # Asegurarse de que la columna 'date' sea de tipo datetime
    df["date"] = pd.to_datetime(df["date"])

    # Establecer 'date' como índice
    df.set_index("date", inplace=True)

    # Realizar la agregación
    aggregated_df = df.resample(freq).agg({"brent_price": agg_func}).reset_index()

    logger.info(f"Agregación completada - {len(aggregated_df):,} períodos generados")

    return aggregated_df


@timer
def agg_brent_price_for_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos de Brent para la tabla de analytics con estadísticas completas.

    Args:
        df: DataFrame con columnas ['date', 'brent_price']

    Returns:
        DataFrame con columnas ['year', 'month', 'avg_brent_price_usd', 'min_brent_price_usd',
                                'max_brent_price_usd', 'record_count']
    """
    logger.info("Agregando datos de Brent para analytics - Frecuencia: mensual")

    # Hacer una copia
    df = df.copy()

    # Asegurar que date sea datetime
    df["date"] = pd.to_datetime(df["date"])

    # Extraer year y month
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Agregar por año y mes
    aggregated_df = (
        df.groupby(["year", "month"])
        .agg(
            avg_brent_price_usd=("brent_price", "mean"),
            min_brent_price_usd=("brent_price", "min"),
            max_brent_price_usd=("brent_price", "max"),
            record_count=("brent_price", "count"),
        )
        .reset_index()
    )

    logger.info(f"Agregación completada - {len(aggregated_df):,} períodos generados")

    return aggregated_df


@timer
def process_brent_price_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline para procesar y transformar datos del precio del Brent
    """

    # Paso 1: Limpiar
    df_cleaned = clean_brent_price(raw_df)

    # Paso 2: Transformar y agregar
    df_transformed = agg_brent_price(df_cleaned)

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

    # Limpieza
    before_cleaning = len(cleaned_df)
    cleaned_df = cleaned_df.dropna(subset=["periodo", "precio_surtidor"])
    nulls_removed = before_cleaning - len(cleaned_df)
    if nulls_removed > 0:
        logger.info(
            f"  Eliminados {nulls_removed:,} registros con valores nulos en periodo/precio"
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

    df_aggregated = df_selected.groupby(
        ["periodo", "provincia", "bandera", "producto"], as_index=False
    ).agg(
        precio_surtidor_mediana=("precio_surtidor", "median"),
        volumen_total=("volumen", "sum"),
    )

    logger.info(f"Agregación completada - {len(df_aggregated):,} registros agregados")
    logger.info(f"  Combinaciones únicas:")
    logger.info(f"     - Periodos: {df_aggregated['periodo'].nunique()}")
    logger.info(f"     - Provincias: {df_aggregated['provincia'].nunique()}")
    logger.info(f"     - Banderas: {df_aggregated['bandera'].nunique()}")
    logger.info(f"     - Productos: {df_aggregated['producto'].nunique()}")

    return df_aggregated


@timer
def fuel_price_aggs_for_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos de combustibles para analytics con year y month.

    Args:
        df: DataFrame con datos ya agregados de fuel_price_aggs

    Returns:
        DataFrame con columnas ['year', 'month', 'provincia', 'bandera', 'producto',
                                'precio_surtidor_mediana', 'volumen_total']
    """
    logger.info(f"Preparando datos de combustibles para analytics")

    # Hacer copia
    df = df.copy()

    # Asegurar que periodo sea datetime
    df["periodo"] = pd.to_datetime(df["periodo"])

    # Extraer year y month
    df["year"] = df["periodo"].dt.year
    df["month"] = df["periodo"].dt.month

    # Seleccionar columnas en el orden correcto
    result_df = df[
        [
            "year",
            "month",
            "provincia",
            "bandera",
            "producto",
            "precio_surtidor_mediana",
            "volumen_total",
        ]
    ].copy()

    logger.info(f"Preparación completada - {len(result_df):,} registros")

    return result_df


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

    # Opcional: Guardar staging
    if save_staging:
        logger.info("\nPASO 2: Guardando datos en staging")
        staging_path = Path(__file__).parent.parent.parent / "data" / "raw"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            save_to_parquet(cleaned_df, staging_path, f"fuel_clean_{timestamp}")
        except Exception as e:
            logger.warning(f"  No se pudo guardar staging: {e}")

    # Paso 2: Transformar
    logger.info("\nPASO 3: Agregación de datos")
    transformed_df = fuel_price_aggs(cleaned_df)

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
    """
    logger.info(f"Iniciando limpieza de USD/ARS - Registros iniciales: {len(df):,}")

    df = df.copy()

    # Rename date column
    df = df.rename(columns={"fecha": "date"})
    logger.debug("  Columna 'fecha' renombrada a 'date'")

    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    # Remove rows with null dates
    before_cleaning = len(df)
    df = df.dropna(subset=["date"])
    nulls_removed = before_cleaning - len(df)
    if nulls_removed > 0:
        logger.info(f"  Eliminados {nulls_removed:,} registros con fechas nulas")

    logger.info(f"Limpieza completada - Registros finales: {len(df):,}")
    logger.info(f"  Rango: {df['date'].min().date()} a {df['date'].max().date()}")

    return df


@timer
def dollar_price_aggs(
    df: pd.DataFrame, freq: str = "M", agg_func: Callable = np.mean
) -> pd.DataFrame:
    """
    Realiza agregaciones en el DataFrame de precios del dólar Blue y Oficial.

    Args:
        df (pd.DataFrame): DataFrame con los datos originales.
        freq (str): Frecuencia de agregación ('M' para mensual, 'Q' para trimestral, etc.).
        agg_func (Callable): Función de agregación (por defecto np.mean).

    Returns:
        pd.DataFrame: DataFrame con las agregaciones realizadas.
    """
    freq_map = {
        "D": "diaria",
        "W": "semanal",
        "M": "mensual",
        "Q": "trimestral",
        "Y": "anual",
    }
    freq_name = freq_map.get(freq, freq)

    logger.info(f"Agregando datos de USD/ARS - Frecuencia: {freq_name}")

    # Hacer una copia para no modificar el DataFrame original
    df = df.copy()

    # Asegurarse de que la columna 'date' sea de tipo datetime
    df["date"] = pd.to_datetime(df["date"])

    # Establecer 'date' como índice
    df.set_index("date", inplace=True)

    # Realizar la agregación
    aggregated_df = df.resample(freq).agg(agg_func).reset_index()

    logger.info(f"Agregación completada - {len(aggregated_df):,} períodos generados")

    return aggregated_df


@timer
def dollar_price_aggs_for_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos de USD/ARS para analytics con year y month.

    Args:
        df: DataFrame con columnas ['date', 'usd_ars_oficial', 'usd_ars_blue', 'brecha_cambiaria_pct']

    Returns:
        DataFrame con columnas ['year', 'month', 'avg_usd_ars_oficial', 'avg_usd_ars_blue',
                                'avg_brecha_cambiaria_pct', 'record_count']
    """
    logger.info("Agregando datos de USD/ARS para analytics - Frecuencia: mensual")

    # Hacer copia
    df = df.copy()

    # Asegurar que date sea datetime
    df["date"] = pd.to_datetime(df["date"])

    # Extraer year y month
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Agregar por año y mes
    aggregated_df = (
        df.groupby(["year", "month"])
        .agg(
            avg_usd_ars_oficial=("usd_ars_oficial", "mean"),
            avg_usd_ars_blue=("usd_ars_blue", "mean"),
            avg_brecha_cambiaria_pct=("brecha_cambiaria_pct", "mean"),
            record_count=("usd_ars_oficial", "count"),
        )
        .reset_index()
    )

    logger.info(f"Agregación completada - {len(aggregated_df):,} períodos generados")

    return aggregated_df


@timer
def process_dolar_price_data(raw_df: pd.DataFrame):
    """
    Pipeline para limpiar y transformar datos del precio del dolar oficial y blue
    """

    # Paso 1: Limpiar
    df_cleaned = clean_dollar_price(raw_df)

    # Paso 2: Transformar y agregar
    df_transformed = dollar_price_aggs(df_cleaned)

    return df_transformed


#####################################################
# OTRAS FUNCIONES
#####################################################


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
    logger.info(f"Tamaño: {file_path.stat().st_size / 1024 / 1024:.2f} MB")

    return file_path


# Probar las funciones de transformación
if __name__ == "__main__":
    from pathlib import Path

    # Obtener la ruta correcta al directorio de datos
    project_root = Path(__file__).parent.parent.parent
    data_path = project_root / "data" / "raw"

    print("=" * 70)
    print("PRUEBA DE TRANSFORMACIONES")
    print("=" * 70)
    print(f"Directorio de datos: {data_path}")
    print("=" * 70)

    # 1. Prueba de Brent
    print("\n[1/3] Probando transformaciones de Brent...")
    print("-" * 70)
    brent_file = data_path / "brent_prices.csv"
    if not brent_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {brent_file}")
    else:
        brent_raw = pd.read_csv(brent_file)

    brent_clean = clean_brent_price(brent_raw)
    brent_monthly = agg_brent_price(brent_clean)

    print("Datos originales de Brent:")
    print(brent_raw.head())
    print(f"\nDatos limpios: {len(brent_clean)} registros")
    print(brent_clean.head())
    print(f"\nDatos agregados mensualmente: {len(brent_monthly)} meses")
    print(brent_monthly.head())

    # 2. Prueba de Dólar
    print("\n[2/3] Probando transformaciones de Dólar...")
    print("-" * 70)
    dollar_file = data_path / "usd_ars_bluelytics.csv"
    if not dollar_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {dollar_file}")
    else:
        dollar_raw = pd.read_csv(dollar_file)

    dollar_clean = clean_dollar_price(dollar_raw)
    dollar_monthly = dollar_price_aggs(dollar_clean, freq="M")

    print("Datos originales de Dólar:")
    print(dollar_raw.head())
    print(f"\nDatos limpios: {len(dollar_clean)} registros")
    print(dollar_clean.head())
    print(f"\nDatos agregados mensualmente: {len(dollar_monthly)} meses")
    print(dollar_monthly.head())

    # 3. Prueba de Combustibles
    print("\n[3/3] Probando transformaciones de Combustibles...")
    print("-" * 70)
    fuel_file = data_path / "precios_eess_completo.csv"
    if not fuel_file.exists():
        print(f"ADVERTENCIA: Archivo no encontrado: {fuel_file}")
    else:
        fuel_raw = pd.read_csv(fuel_file)

    fuel_clean = clean_fuel_price(fuel_raw)

    print(f"\nDatos limpios: {len(fuel_clean)} registros")
    print(fuel_clean.head())
    print("Productos únicos luego de la limpieza")
    print(fuel_clean["producto"].unique())

    fuel_transformed = fuel_price_aggs(fuel_clean)

    print("\nDataFrame Agrupado")
    print(fuel_transformed.head())

    print("\n" + "=" * 70)
    print("TODAS LAS TRANSFORMACIONES COMPLETADAS EXITOSAMENTE")
    print("=" * 70)
